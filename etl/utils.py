from termcolor import cprint
from halo import Halo


class Output:
    DEFAULT_COLOR = 'white'
    INFO_COLOR = 'blue'
    WARNING_COLOR = 'yellow'
    ERROR_COLOR = 'red'
    SUCCESS_COLOR = 'green'

    def __init__(self):
        self.types_colors = {
            'default': self.DEFAULT_COLOR,
            'info': self.INFO_COLOR,
            'warning': self.WARNING_COLOR,
            'error': self.ERROR_COLOR,
            'success': self.SUCCESS_COLOR
        }

        self.sp = Halo()

    def start_spinner(self, message: str, color: str = None):
        self.sp.text_color = color if color else self.DEFAULT_COLOR
        self.sp.start(text=message)

    def spinner_success(self, message: str = None, color: str = None, message_color: str = None):
        self.sp.text_color = color if color else self.SUCCESS_COLOR
        self.sp.succeed()

        if message:
            self.sp.text_color = message_color if message_color else self.INFO_COLOR
            self.sp.stop_and_persist(text=message)

    def spinner_fail(self, message: str = None, color: str = None):
        self.sp.text_color = color if color else self.ERROR_COLOR
        self.sp.fail()

        if message:
            self.sp.stop_and_persist(text=message)

    def title(self, message: str, color: str = None):
        if not color:
            color = self.DEFAULT_COLOR

        message = '\n * {} \n{}'.format(message.upper(), '=' * 60)
        cprint(message, color=color, attrs=['bold'])

    def success(self, message: str):
        self.write(message, 'success', '✔')

    def warning(self, message: str):
        self.write(message, 'warning', '⚠')

    def info(self, message: str):
        self.write(message, 'info', 'ℹ')

    def error(self, message: str):
        self.write(message, 'error', '✗')

    def write(self, message: str, message_type: str = 'default', symbol: str = ''):
        color = self.types_colors[message_type]

        if symbol:
            symbol = '{} '.format(symbol)

        print('')
        cprint('{}{}'.format(symbol, message), color=color, attrs=['bold'])
        print('')

