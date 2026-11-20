"""Expected row errors that a pipeline may record and continue past."""


class RowValidationError(ValueError):
    """A parsed row cannot satisfy the fixed target-column contract.

    The pipeline records its source location and reason as a RejectedRecord.
    File decoding, CSV framing and I/O errors propagate separately as fatal.
    """
