class NoAccounts(Exception):
    pass


class TemporarilyBanned(Exception):
    pass


class PermanentlyBanned(Exception):
    pass


class NotAuthorized(Exception):
    pass


class UserFloodWait(Exception):
    pass
