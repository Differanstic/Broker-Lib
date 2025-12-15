import neo_api_client
from . import utils
import pyotp
import json
import pandas as pd
from typing import Tuple, List, Dict, Any
from datetime import datetime
import os


class KotakNeo:
    def __init__(self, config_path=None, consumer_key=None, mobile_number=None, ucc=None, mpin=None, totp_secret=None,record_path = ''):
        if config_path:
            with open(config_path, 'r') as f:
                config = json.load(f)
                kotak_config = config.get('kotak', {})
                self.consumer_key = kotak_config.get('consumer_key')
                self.mobile_number = kotak_config.get('mobile_number')
                self.ucc = kotak_config.get('ucc')
                self.mpin = kotak_config.get('mpin')
                self.totp_secret = kotak_config.get('totp_secret')
        else:
            self.consumer_key = consumer_key
            self.mobile_number = mobile_number
            self.ucc = ucc
            self.mpin = mpin
            self.totp_secret = totp_secret
            
        if not all([self.consumer_key, self.mobile_number, self.ucc, self.mpin, self.totp_secret]):
            raise ValueError("Missing Kotak Neo credentials. Provide them via config_path or direct arguments.")

        self.client = neo_api_client.NeoAPI(environment="PROD", consumer_key=self.consumer_key)
        self.record_path = record_path
        self._login()
        
    def _recorder(self ,res:dict, filename:str):
        df = pd.DataFrame([res])
        df['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.record_path = self.record_path.rstrip("/\\")
        path = os.path.join(self.record_path, filename)
        if not os.path.exists(path):
            df.to_csv(path, index=False)
        else:
            df.to_csv(path, mode='a', header=False, index=False)

    def _login(self):
        totp = pyotp.TOTP(self.totp_secret)
        x = self.client.totp_login(mobile_number=self.mobile_number, ucc=self.ucc, totp=totp.now())
        print(x)
        x = self.client.totp_validate(mpin=self.mpin)
        print(x)
        self._recorder(x,'kotak-login.csv')
        

        
    def place_order(self, exchange_segment, product, price, order_type, quantity, validity, trading_symbol,
                    transaction_type, amo="NO", disclosed_quantity="0", market_protection="0", pf="N",
                    trigger_price="0", tag=None, scrip_token=None, square_off_type=None,
                    stop_loss_type=None, stop_loss_value=None, square_off_value=None,
                    last_traded_price=None, trailing_stop_loss=None, trailing_sl_value=None):
        return self.client.place_order(
            exchange_segment=exchange_segment, product=product, price=price, order_type=order_type,
            quantity=quantity, validity=validity, trading_symbol=trading_symbol,
            transaction_type=transaction_type, amo=amo, disclosed_quantity=disclosed_quantity,
            market_protection=market_protection, pf=pf, trigger_price=trigger_price, tag=tag,
            scrip_token=scrip_token, square_off_type=square_off_type, stop_loss_type=stop_loss_type,
            stop_loss_value=stop_loss_value, square_off_value=square_off_value,
            last_traded_price=last_traded_price, trailing_stop_loss=trailing_stop_loss,
            trailing_sl_value=trailing_sl_value
        )
    
    def place_market_order(self,exchange_segment:str,symbol:str,quantity:str,transaction_type:str):
        entry_price = -1
        res = self.place_order(exchange_segment=exchange_segment,trading_symbol=symbol,quantity=quantity,transaction_type=transaction_type,validity="DAY",product='NRML',price="0",order_type="MKT")

        if res['stat'] == 'Ok' or res['stCode'] == 200:
            nOrdNo = res['nOrdNo']
            is_completed, order = self.order_status(nOrdNo=nOrdNo)
            self._recorder(order,'kotak-order.csv')
            if is_completed:
                trade_report = self.trade_report(nOrdNo=nOrdNo)
                entry_price = float(trade_report['avgPrc'])
                
                self._recorder(trade_report,'kotak-trade.csv')
        else:
            print(res)
            
                
        return entry_price

    def modify_order(self, order_id, price, quantity, disclosed_quantity="0", trigger_price="0",
                     validity="DAY", order_type=''):
        return self.client.modify_order(
            order_id=order_id, price=price, quantity=quantity,
            disclosed_quantity=disclosed_quantity, trigger_price=trigger_price,
            validity=validity, order_type=order_type
        )

    def cancel_order(self, order_id):
        return self.client.cancel_order(order_id=order_id)

    
    def order_report(self):
        orders = self.client.order_report()
        return orders
    
    def order_status(self,nOrdNo:str):
        
        orders = self.order_report()
        for order in orders['data']:
            if nOrdNo == order['nOrdNo'] :
                status = (order['ordSt'] == 'complete')
                return status,order

    def trade_report(self,nOrdNo=None):
        if nOrdNo:
            return self.client.trade_report(order_id = nOrdNo)['data']
        else:
            return self.client.trade_report()

    def positions(self):
        return self.client.positions()

    def holdings(self):
        return self.client.holdings()

    def limits(self, segment="ALL", exchange="ALL", product="ALL"):
        return self.client.limits(segment=segment, exchange=exchange, product=product)
    
    def available_funds(self):
        return float(self.limits()['Net'])

    def scrip_master(self, exchange_segment=""):
        return self.client.scrip_master(exchange_segment=exchange_segment)

    def logout(self):
        return self.client.logout()

    
    

    def open_positions(self) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Fetch and return the list of open trading positions.

        A position is considered "open" when:
            flSellQty != flBuyQty

        Returns
        -------
        Tuple[bool, List[Dict[str, Any]]]
            - in_position (bool): True if there is at least one open position.
            - open_positions (list): List of open position dictionaries.

        Notes
        -----
        - This function expects `neo.positions()` to return a dictionary 
          with a 'data' key containing a list of positions.
        - Safe for production: includes error handling & correct filtering.
        """

        open_positions: List[Dict[str, Any]] = []

        try:
            positions = self.positions()

            # Validate response format
            if not isinstance(positions, dict) or "data" not in positions:
                raise ValueError("Invalid response structure from neo.positions()")

            for position in positions["data"]:
                # Check mismatch in buy vs sell quantities
                if position.get("flSellQty") != position.get("flBuyQty"):
                    open_positions.append(position)

            in_position = len(open_positions) > 0
            return in_position, open_positions

        except Exception as e:
            # In production, log the error instead of printing
            print(f"[ERROR] Failed to fetch open positions: {e}")
            return False, []



    def net_pnl(self,bot_trade:bool):
        try:
            data = self.client.order_report()['data']
            keys = ['exCfmTm','exSeg','avgPrc','qty','sym','stkPrc','optTp','trnsTp','stat']
            df = pd.DataFrame(data)
            df = df[keys]
            df.rename(columns={'exCfmTm':'timestamp','exSeg':'segment','avgPrc':'price','sym':'symbol','stkPrc':'strike','optTp':'option_type','trnsTp':'order_type'},inplace=True)

            df['timestamp'] = pd.to_datetime(
                df['timestamp'],
                format='%d-%b-%Y %H:%M:%S',
                errors='coerce'
            )
            df['price'] = pd.to_numeric(df['price'])
            df['qty'] = pd.to_numeric(df['qty'])
            df.sort_values(by='timestamp',inplace=True)


            group_cols = ['symbol', 'strike', 'option_type']

            results = []

            for grp, g in df.groupby(group_cols):
                g = g.reset_index(drop=True)

                buys  = g[g['order_type'] == 'B']
                sells = g[g['order_type'] == 'S']

                # match buy–sell sequentially (FIFO)
                for i in range(min(len(buys), len(sells))):
                    buy_price  = buys.iloc[i]['price']
                    sell_price = sells.iloc[i]['price']
                    qty        = buys.iloc[i]['qty']

                    pnl = (sell_price - buy_price) * qty

                    results.append({
                        "symbol": grp[0],
                        "strike": grp[1],
                        "option_type": grp[2],
                        "buy_time": buys.iloc[i]['timestamp'],
                        "sell_time": sells.iloc[i]['timestamp'],
                        "buy_price": buy_price,
                        "sell_price": sell_price,
                        "qty": qty,
                        "pnl": pnl
                    })

            pnl_df = pd.DataFrame(results)

            pnl_df['charges'] = pnl_df.apply(
                lambda row: utils.calculate_options_charges(
                    row['buy_price'],
                    row['sell_price'],
                    row['qty'],
                    brokerage= 20 if not bot_trade else 0
                )['total_charges'],
                axis=1
            )
            pnl_df['net'] = pnl_df['pnl'] - pnl_df['charges']
        except Exception as e:
            print(f"Error: {e}")
        return pnl_df
    
    
