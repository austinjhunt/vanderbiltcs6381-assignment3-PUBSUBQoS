import socket
class Publisher:
    # What should be part of publisher?
    ip_address = socket.gethostbyname(socket.gethostname())
    topics = []

    # Maybe define a maximum number of topics per publisher
    max_num_topics = 0

    # Assumption:
    # Publisher is only going to publish a limited number of topics.
    #