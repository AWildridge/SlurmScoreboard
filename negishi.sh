
export SLURM_SB_ROOT=/depot/cms/top/awildrid/SlurmScoreboard 
export PATH=$SLURM_SB_ROOT/bin:$PATH
nohup slurm-sb-daemon --cluster negishi --interval 600 > negishi_daemon.log 2>&1 & echo $! > negishi_daemon.pid disown
