FROM node:20-alpine
WORKDIR /verify
COPY verify.mjs .
ENV API_URL=http://api:8080 DATA_DIR=/data
CMD ["node", "verify.mjs"]
