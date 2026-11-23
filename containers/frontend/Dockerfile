FROM node:22.22.3-alpine AS development
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
COPY src/pisces/schemas/ /app/src/pisces/schemas/
COPY assets/brand/ /app/assets/brand/
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "8080"]

FROM development AS build
RUN npm run build

FROM nginx:1.28-alpine AS production
COPY containers/frontend/nginx.conf /etc/nginx/nginx.conf
COPY containers/frontend/default.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/frontend/dist/ /usr/share/nginx/html/
USER 101:101
EXPOSE 8080
