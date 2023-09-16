class FileEntryNotFoundError(FileNotFoundError):
    pass


class InvalidEnumerationError(ValueError):
    pass


class UnprocessedFileNotFoundError(FileNotFoundError):
    pass


class ProcessingError(ValueError):
    pass
