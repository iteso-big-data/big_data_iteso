class BankAccountManager:
    def __init__(self, balance):
        self._balance = balance

    def __repr__(self):
        return f"Account(balance){self._balance}"