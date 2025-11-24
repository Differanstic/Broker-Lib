## Installation
```bash
pip install -r requirements.txt
```

## Usage
```python
## Installation
```bash
pip install -r requirements.txt
```

## Usage
```python
from Broker.fyers import fyers_util
from Broker.kotak_neo import KotakNeo

# Initialize with config file path
config_path = "path/to/config.json"

fyers = fyers_util(config_path=config_path)
kotak_neo = KotakNeo(config_path=config_path)
``` 
## Configuration
You can store your credentials in a `config.json` file. Use `config.json.example` as a template.

```json
{
    "fyers": {
        "client_id": "YOUR_CLIENT_ID",
        "secret_key": "YOUR_SECRET_KEY",
        "pin": "YOUR_PIN"
    },
    "kotak": {
        "consumer_key": "YOUR_CONSUMER_KEY",
        "mobile_number": "YOUR_MOBILE_NUMBER",
        "ucc": "YOUR_UCC",
        "mpin": "YOUR_MPIN",
        "totp_secret": "YOUR_TOTP_SECRET"
    }
}
```