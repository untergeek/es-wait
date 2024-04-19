"""Args Class"""
import typing as t

class Args(t.Dict):
    """
    Initialize with None values for all accepted settings

    Contains :py:meth:`update_settings` and :py:meth:`asdict` methods
    """
    def __init__(
            self,
            settings: t.Dict[str, t.Any] = None,
            defaults: t.Dict[str, t.Any] = None,
        ):
        """Updatable object that turns dictionary keys into properties"""
        self.settings = settings
        if defaults is None:
            defaults = {}
        self.set_defaults(defaults)
        if settings is None:
            self.settings = defaults
        else:
            # Only do this if we sent actual settings
            self.update_settings(self.settings)

    def set_defaults(self, defaults: dict) -> None:
        """Set attr values from defaults"""
        for key, value in defaults.items():
            setattr(self, key, value)
            # Override self.settings if no default for key is found
            if key not in self.settings:
                self.settings[key] = value

    def update_settings(self, new_settings: dict) -> None:
        """Update individual settings from provided new_settings dict"""
        for key, value in new_settings.items():
            setattr(self, key, value)

    @property
    def asdict(self) -> dict:
        """Return as a dictionary"""
        retval = {}
        if isinstance(self.settings, dict):
            for setting in self.settings:
                retval[setting] = getattr(self, setting, None)
        return retval

class TaskArgs(Args):
    """Task-specific child class of Args"""
    def __init__(
            self,
            settings: t.Dict[str, t.Any] = None,
            defaults: t.Dict[str, t.Any] = None,
        ):
        super().__init__(settings=settings, defaults=defaults)
        self.action = None
        self.completed = False
        self.description = None
        self.response = {}
        self.running_time_in_nanos = 0
        self.task = None
        self.task_data = None
