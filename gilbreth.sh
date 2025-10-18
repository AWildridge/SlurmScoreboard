
export SLURM_SB_ROOT=/depot/cms/top/awildrid/SlurmScoreboard 
export PATH=$SLURM_SB_ROOT/bin:$PATH
nohup slurm-sb-daemon --cluster gilbreth --interval 600 > gilbreth_daemon.log 2>&1 & echo $! > gilbreth_daemon.pid disown
