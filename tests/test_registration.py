import json
import time
from kafka import KafkaProducer
import uuid
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(
        login="string",
        email=expected_mail,
        password="string"
    )
    for _ in range(10):
        response = mail.find_message(expected_mail)
        if response.json()['total'] > 0:
            break
        else:
            raise AssertionError("Email is not found")

def test_success_registration(account: AccountApi, mail: MailApi) -> None:
    base = uuid.uuid4().hex
    expected_mail = f"{base}@mail.ru"
    account.register_user(
        login=base,
        email=expected_mail,
        password="string"
    )
    for _ in range(10):
        response = mail.find_message(expected_mail)
        if response.json()['total'] > 0:
            break
        else:
            raise AssertionError("Email is not found")


def test_success_registration_with_kafka_producer(mail: MailApi) -> None:
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123"
    }
    producer = KafkaProducer(
        bootstrap_servers=['185.185.143.231:9092'],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=5000,
        request_timeout_ms=70000,
        connections_max_idle_ms=65000,
        reconnect_backoff_ms=5000,
        reconnect_backoff_max_ms=10000
    )
    producer.send('register-events',message)
    producer.close()
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()['total'] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError('Email is not found')