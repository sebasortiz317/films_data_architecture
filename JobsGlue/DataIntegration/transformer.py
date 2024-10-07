from abc import ABC, abstractmethod
from awsglue.dynamicframe import DynamicFrame

class Transformer(ABC):
    """
    Clase abstracta base para todos los transformadores de hojas.
    """
    @abstractmethod
    def transform(self, dynamic_frame: DynamicFrame):
        """
        Método abstracto para transformar un DynamicFrame.
        Debe ser implementado por cada transformador específico.
        """
        pass
