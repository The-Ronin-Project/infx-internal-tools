from dataclasses import dataclass


@dataclass
class Organization:
    """ Also known as 'tenant' at Ronin """
    id: str

    def __eq__(self, other):
        if isinstance(other, Organization):
            return self.id == other.id
        return False

    def __hash__(self):
        return hash(self.id)