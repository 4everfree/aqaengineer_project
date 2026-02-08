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