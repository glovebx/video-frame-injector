#!/bin/bash

# ========== 配置变量（请根据实际情况微调）==========
SERVICE_NAME="metaerpvideoinjector"         # 服务名称
USER_NAME="admin"                           # 运行服务的用户
GROUP_NAME="admin"                          # 运行服务的用户组
CONDA_BIN="/home/admin/miniconda3/condabin/conda"   # conda 可执行文件路径
CONDA_ENV="metaerp"                         # conda 环境名称
WORK_DIR="/var/www/video-frame-injector"      # 代码所在目录
PYTHON_CMD="python api.py"             # 启动命令（注意：conda run 会自动使用环境中的 python）
# ===============================================

# 检查 conda 是否存在
if [ ! -f "$CONDA_BIN" ]; then
    echo "错误: 找不到 conda 可执行文件: $CONDA_BIN"
    exit 1
fi

# 检查工作目录是否存在
if [ ! -d "$WORK_DIR" ]; then
    echo "错误: 工作目录不存在: $WORK_DIR"
    exit 1
fi

# 创建 systemd 服务文件
sudo tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null <<EOF
[Unit]
Description=MetaERP Video Scene Processor
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER_NAME}
Group=${GROUP_NAME}
WorkingDirectory=${WORK_DIR}
ExecStart=${CONDA_BIN} run -n ${CONDA_ENV} ${PYTHON_CMD}
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

[Install]
WantedBy=multi-user.target
EOF

# 重新加载 systemd 配置
sudo systemctl daemon-reload

# 启用开机自启
sudo systemctl enable ${SERVICE_NAME}

# 启动服务
sudo systemctl start ${SERVICE_NAME}

# 检查服务状态
sudo systemctl status ${SERVICE_NAME} --no-pager

echo "========================================="
echo "服务部署完成！"
echo "常用命令："
echo "  启动: sudo systemctl start ${SERVICE_NAME}"
echo "  停止: sudo systemctl stop ${SERVICE_NAME}"
echo "  重启: sudo systemctl restart ${SERVICE_NAME}"
echo "  状态: sudo systemctl status ${SERVICE_NAME}"
echo "  日志: sudo journalctl -u ${SERVICE_NAME} -f"
echo "========================================="