# 情侣联机小游戏 · WebSocket 服务端
# 抖音云容器服务：容器内监听端口必须是 8000
# 基础镜像用抖音云推荐的国内镜像源（免翻墙、拉取快）
FROM i.public-cn-beijing.cr.volces.com/public/node:18-alpine

ENV NODE_ENV=production \
    PORT=8000

WORKDIR /app

# 先只拷依赖清单，让安装这一层能被 Docker 缓存
COPY package.json package-lock.json ./

# npm ci 严格按 lock 文件装、版本可复现；--omit=dev 只装运行时依赖
RUN npm ci --omit=dev --no-audit --no-fund

# 再拷代码，改代码不会让上面的 install 层失效
COPY server.js ./

# 非 root 运行：node 官方镜像自带 uid=1000 的 node 用户
USER node

EXPOSE 8000

CMD ["node", "server.js"]
