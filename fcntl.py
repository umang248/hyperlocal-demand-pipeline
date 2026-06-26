# Mock fcntl module for running Apache Airflow on Windows
import sys
import signal

# Ensure this mock is only used on Windows
if sys.platform == 'win32':
    # Mock fcntl functions
    def fcntl(fd, op, arg=0):
        return 0

    def ioctl(fd, op, arg=0):
        return 0

    def flock(fd, op):
        return 0

    def lockf(fd, op, length=0, start=0, whence=0):
        return 0

    # fcntl constants
    DN_ACCESS = 1
    DN_MODIFY = 2
    DN_CREATE = 4
    DN_DELETE = 8
    DN_RENAME = 16
    DN_ATTRIB = 32
    DN_MULTISHOT = 2147483648

    F_DUPFD = 0
    F_GETFD = 1
    F_SETFD = 2
    F_GETFL = 3
    F_SETFL = 4
    F_GETLK = 5
    F_SETLK = 6
    F_SETLKW = 7
    F_GETOWN = 9
    F_SETOWN = 8
    F_RDLCK = 0
    F_WRLCK = 1
    F_UNLCK = 2

    LOCK_SH = 1
    LOCK_EX = 2
    LOCK_NB = 4
    LOCK_UN = 8

    # Patch signal module to bypass Unix SIGALRM timeouts on Windows
    if not hasattr(signal, 'SIGALRM'):
        signal.SIGALRM = 14  # standard Unix SIGALRM signal number
        
        original_signal_func = signal.signal
        
        def patched_signal(sig, handler):
            if sig == 14:  # SIGALRM
                # Return a dummy handler to prevent OS exceptions on Windows
                return None
            return original_signal_func(sig, handler)
            
        signal.signal = patched_signal
        
    if not hasattr(signal, 'alarm'):
        def alarm(seconds):
            return 0
        signal.alarm = alarm
