class MemoryManager:

    def __init__(self, available_memory: int, memory_percentage: int):
        self.available_memory = available_memory
        self.used_memory = 0
        self.memory_percentage = memory_percentage

    def update_used_memory(self, used_memory: int):
        self.used_memory = used_memory
        return self.used_memory

    def has_available_memory(self):
        return self.used_memory < self.available_memory * self.memory_percentage / 100

    def get_used_memory_percentage(self):
        return self.used_memory / self.available_memory * 100
