
export SLURM_SB_ROOT=/depot/cms/top/awildrid/SlurmScoreboard 
export PATH=$SLURM_SB_ROOT/bin:$PATH
nohup slurm-sb-daemon --cluster bell --interval 600 > bell_daemon.log 2>&1 & echo $! > bell_daemon.pid disown
