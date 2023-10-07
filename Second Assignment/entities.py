class Document:
    
    def __init__(self, document_id, document_norm, document_length):
        self.document_id = document_id
        self.document_norm = document_norm
        self.document_length = document_length

    def __eq__(self, other):
        return self.document_id == other.document_id