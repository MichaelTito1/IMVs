FROM postgres:16

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential make git unzip \
      postgresql-server-dev-16 clang llvm \
      ca-certificates python3 python3-pip \
      python3-psycopg2 python3-pandas python3-numpy \
      python3-matplotlib python3-seaborn \
     python3-openpyxl python3-tqdm python3-ijson python3-psutil \
 && rm -rf /var/lib/apt/lists/*

# Build and install pg_ivm
RUN git clone https://github.com/sraoss/pg_ivm.git /pg_ivm \
 && cd /pg_ivm \
 && make && make install \
 && cd / && rm -rf /pg_ivm

# build and install mv_stats
RUN git clone https://github.com/asotolongo/mv_stats.git /mv_stats \
 && cd /mv_stats \
 && make install \
 && cd / && rm -rf /mv_stats

# Expose port and use default postgres startup
EXPOSE 5432
