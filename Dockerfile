FROM flink:1.20.2

# Install software-properties-common to manage PPAs (with PPA, it pulls in Python 3.11 Release Candidate)
RUN apt-get update -y \
    && apt-get install -y \
        software-properties-common \
        curl \
        gnupg \
        lsb-release

# install python3 and pip3
RUN add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update -y \
    && apt-get install -y \
        python3.11 \
        python3-pip \
        python3.11-dev \
    && rm -rf /var/lib/apt/lists/*
RUN ln -sf /usr/bin/python3.11 /usr/bin/python \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3

# install PyFlink
RUN pip3 install apache-flink==1.20.2