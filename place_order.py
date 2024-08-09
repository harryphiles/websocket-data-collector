import base64
import time
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import requests
from config import RSA_KEY_REGISTERED


TESTNET_BASE_URL = "https://testnet.binance.vision"
API_KEY = RSA_KEY_REGISTERED
PRIVATE_KEY_PATH = "SECRETS_EXAMPLE/test-prv-key.pem.example"


def load_private_key(key_path):
    with open(key_path, "rb") as f:
        return load_pem_private_key(data=f.read(), password=None)


def get_timestamp():
    return int(time.time() * 1000)


def sign_request(private_key, payload):
    signature = private_key.sign(
        payload.encode("utf-8"), padding.PKCS1v15(), hashes.SHA256()
    )
    return base64.b64encode(signature)


def prepare_order_params(order_data):
    params = {
        "symbol": order_data[0],
        "side": order_data[1],
        "type": order_data[2],
        "timeInForce": order_data[3],
        "quantity": order_data[4],
        "price": order_data[5],
        "timestamp": get_timestamp(),
    }
    return params


def create_payload(params):
    return "&".join([f"{param}={value}" for param, value in params.items()])


def send_order_request(url, headers, params):
    response = requests.post(url, headers=headers, data=params)
    return response.json()


def place_single_order(private_key, order_data):
    params = prepare_order_params(order_data)
    payload = create_payload(params)
    signature = sign_request(private_key, payload)
    params["signature"] = signature

    headers = {"X-MBX-APIKEY": API_KEY}
    url = f"{TESTNET_BASE_URL}/api/v3/order"

    return send_order_request(url, headers, params)


def place_multiple_orders(orders):
    private_key = load_private_key(PRIVATE_KEY_PATH)
    results = []

    for order in orders:
        result = place_single_order(private_key, order)
        results.append(result)

    return results


def main():
    orders_to_place = [
        ["BTCUSDT", "BUY", "LIMIT", "GTC", "0.0020000", "54662"],
        ["ETHUSDT", "SELL", "LIMIT", "GTC", "0.0100000", "3500"],
        ["BNBUSDT", "BUY", "LIMIT", "GTC", "0.0500000", "450"],
        # ["BTCUSDT", "BUY", "LIMIT", "GTC", "0.0020000", "54662"],
        # ["ETHUSDT", "SELL", "LIMIT", "GTC", "0.0100000", "3500"],
        # ["BNBUSDT", "BUY", "LIMIT", "GTC", "0.0500000", "450"],
        # ["BTCUSDT", "BUY", "LIMIT", "GTC", "0.0020000", "54662"],
        # ["ETHUSDT", "SELL", "LIMIT", "GTC", "0.0100000", "3500"],
        # ["BNBUSDT", "BUY", "LIMIT", "GTC", "0.0500000", "450"],
    ]

    results = place_multiple_orders(orders_to_place)
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
