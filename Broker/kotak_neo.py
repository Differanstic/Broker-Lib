import neo_api_client
import os
import pyotp
import json

class KotakNeo:
    def __init__(self, config_path=None, consumer_key=None, mobile_number=None, ucc=None, mpin=None, totp_secret=None):
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
        self._login()

    def _login(self):
        totp = pyotp.TOTP(self.totp_secret)
        x = self.client.totp_login(mobile_number=self.mobile_number, ucc=self.ucc, totp=totp.now())
        print(x)
        x = self.client.totp_validate(mpin=self.mpin)
        print(x)
        

        
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
        return self.client.order_report()

    def trade_report(self):
        return self.client.trade_report()

    def positions(self):
        return self.client.positions()

    def holdings(self):
        return self.client.holdings()

    def limits(self, segment="ALL", exchange="ALL", product="ALL"):
        return self.client.limits(segment=segment, exchange=exchange, product=product)

    def scrip_master(self, exchange_segment=""):
        return self.client.scrip_master(exchange_segment=exchange_segment)

    def logout(self):
        return self.client.logout()

