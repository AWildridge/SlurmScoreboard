
export SLURM_SB_ROOT=/depot/cms/top/awildrid/SlurmScoreboard 
export PATH=$SLURM_SB_ROOT/bin:$PATH
nohup slurm-sb-daemon --cluster hammer --interval 60 > hammer_daemon.log 2>&1 & echo $! > hammer_daemon.pid disown
