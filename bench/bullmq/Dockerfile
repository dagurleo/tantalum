# The bench image (../../Dockerfile) plus Node and BullMQ, for run.sh.
#
#   docker build -t tantalum-bench .                        # from the repo root
#   docker build -t tantalum-bullmq bench/bullmq
#   docker run -d --name bmq --privileged -v $PWD:/tantalum -e ROLE=idle tantalum-bullmq
#   docker exec -i bmq bash /tantalum/bench/bullmq/run.sh
FROM tantalum-bench
RUN apt-get update \
 && apt-get install -y --no-install-recommends nodejs npm \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /bullmq
COPY package.json jobs.mjs ./
RUN npm install --omit=dev --no-audit --no-fund --loglevel=error && node --version
