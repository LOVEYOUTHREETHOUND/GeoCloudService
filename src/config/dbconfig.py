# 此配置用于腾讯云服务器
database = {
    'user': 'JGF_GXFW',
    'password' : 'JGF_GXFW',
    'host' : '62.234.192.247',
    'database' : 'ORCLCDB',
    'port' : '18881'
}

# 此配置用于Windows Server
# database = {
#     'user': 'jgf_gxfw',
#     'password' : 'icw3kx45',
#     'host' : '10.82.8.4',
#     'database' : 'satdb',
#     'port' : '1521'
# }

PoolDB = {
    'mincached' : 5,
    'maxcached' : 30,
    'maxshared' : 30,
    'maxconnections' : 50,
    'blocking' : True
}