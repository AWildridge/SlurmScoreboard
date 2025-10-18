
export SLURM_SB_ROOT=/depot/cms/top/awildrid/SlurmScoreboard 
export PATH=$SLURM_SB_ROOT/bin:$PATH
nohup slurm-sb-daemon --cluster gautschi --interval 600 > gautschi_daemon.log 2>&1 & echo $! > gautschi_daemon.pid disown
