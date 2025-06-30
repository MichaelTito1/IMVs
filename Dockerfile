FROM postgres:15

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential make git unzip \
      postgresql-server-dev-15 clang llvm \
      ca-certificates python3 python3-pip \
      python3-psycopg2 python3-pandas python3-numpy \
 && rm -rf /var/lib/apt/lists/*

# Build and install pg_ivm
RUN git clone https://github.com/sraoss/pg_ivm.git /pg_ivm \
 && cd /pg_ivm \
 && make && make install \
 && cd / && rm -rf /pg_ivm

# Expose port and use default postgres startup
EXPOSE 5432
