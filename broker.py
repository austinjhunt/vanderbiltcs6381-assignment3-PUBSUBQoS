"""
Message Broker to serve as anonymizing middleware between
publishers and subscribers
"""

class Broker:
    subscribers = {
        'topic': [] # list of subscriber addresses subscribed to this topic
    }
    publishers = {
        'topic': [
            # list of publisher addresses publishing this topic
        ]
    }

    # when someone subscribes to a topic "XYZ",
    def register_pub(topic="", publisher_address=""):
        # Should broker always be listening for these registration requests?
        # Can we take the address of the publisher (requestor)
        # without them explicitly passing it?
        pass

    def register_sub(topic="", subscriber_address=""):
        # Should broker always be listening for these registration requests?
        pass




