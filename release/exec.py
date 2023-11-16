import paramiko

remote_hosts = []

def execute_remote_command(host, port, username, key_filename, command):
    # 创建SSH客户端
    client = paramiko.SSHClient()

    # 设置客户端加载系统SSH密钥
    client.load_system_host_keys()

    # 添加远程主机的SSH密钥
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # 连接远程主机
        client.connect(host, port, username=username, key_filename=key_filename)

        # 执行远程命令
        stdin, stdout, stderr = client.exec_command(command)

        # 打印命令执行结果
        print("Command Output:")
        print(stdout.read().decode('utf-8'))

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # 关闭SSH连接
        client.close()

# 替换以下信息为您的实际值
remote_host = '172.31.13.109'
remote_port = 22  # 默认SSH端口
remote_username = 'hadoop'
private_key_path = '/home/ec2-user/work/stathbase/ec2-global-media.pem'
remote_command = 'aws s3 cp s3://tx-example-data/hbase2/func/stathbase-0.0.1.jar ~/optt.jar'

# 执行远程命令
execute_remote_command(remote_host, remote_port, remote_username, private_key_path, remote_command)
