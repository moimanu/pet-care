class DomainError(Exception):
    """Base exception for pet-care domain errors."""
    pass


class SanityError(DomainError):
    """Raised when telemetry or command inputs violate domain invariants."""
    pass


class QuorumError(DomainError):
    """Raised when quorum cannot be reached among distributed controller nodes."""
    pass


class NodeUnreachableError(DomainError):
    """Raised when communication with a controller node fails."""
    pass
