
class NoAccountException(Exception):

    def __init__(self, message='[EGOVBENCH_EXCEPTION]> ACCOUNT DOES NOT EXIST / HAS BEEN DEPRECATED, PLEASE INSERT THE CORRECT ONE'):
        super().__init__(message)
