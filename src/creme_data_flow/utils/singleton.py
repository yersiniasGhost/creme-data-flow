print(f"[MODULE LOAD] singleton.py being loaded from: {__file__}")

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            try:
                instance = super(Singleton, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
            except Exception as e:
                # Don't cache failed initialization attempts
                print(f"[SINGLETON DEBUG] Initialization failed: {e}")
                raise

        return cls._instances[cls]
