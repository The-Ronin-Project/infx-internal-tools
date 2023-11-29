from abc import ABC, abstractmethod
from typing import Callable

import app.models


class DeferredPublisher(ABC):
    @abstractmethod
    def prepare_artifact_for_publication(self):
        pass

    @abstractmethod
    def publish_prepared_artifact(self):
        """
        Subclasses implementing this method MUST use it ONLY to publish a previously-generated artifact.
        As this method will be executed AFTER the database transaction is already committed, no changes to the database are allowed.
        """
        pass


class DeferredPublicationManager:
    instance = None

    class __DeferredPublicationInstance:
        def __init__(self):
            self.publish_queue = []
            self.will_regenerate_data_normalization_registry = False

        def publish(self):
            # Likely needs try/catch and some exception handling

            if self.will_regenerate_data_normalization_registry:
                self.regenerate_data_normalization_registry()
            self.will_regenerate_data_normalization_registry = False

            for item in self.publish_queue:
                item()
            self.publish_queue = []

        def add_to_publish_queue(self, publish_method: Callable):
            self.publish_queue.append(publish_method)

        def purge_publish_queue(self):
            self.publish_queue = []

        def queue_data_normalization_registry_regeneration(self):
            self.will_regenerate_data_normalization_registry = True

        def regenerate_data_normalization_registry(self):
            # Publish new version of data normalization registry
            app.models.data_ingestion_registry.DataNormalizationRegistry.publish_data_normalization_registry()

    def __new__(cls):
        if not DeferredPublicationManager.instance:
            DeferredPublicationManager.instance = DeferredPublicationManager.__DeferredPublicationInstance()
        return DeferredPublicationManager.instance

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name, value):
        return setattr(self.instance, name, value)