#!/bin/sh
export RADICAL_ENGINE_VERBOSE=INFO 
#export RADICAL_WLMS_VERBOSE=INFO 
#export RADICAL_EXECUTOR_VERBOSE=INFO 
trials=10
dest='res_dyn'
mean_range="16 32 64"
spread_range="0 6.25 12.5 25"
conc_range="128"
gens_range="8"
log_path=`pwd`
sleep_dur=20
rm $log_path/progress.log -rf

# Res Dyn exps
for c in `seq 1 4`
do
	cd $dest/case-$c
	cp ../../runme_$dest.py .
	for mean in $mean_range
	do
		for spread in $spread_range
		do
			for conc in $conc_range
			do
				for gens in $gens_range
				do
					echo "$dest: Started case-$c: $mean $spread $conc $gens" >> $log_path/progress.log
					python runme_$dest.py --wl_mean 2048 --wl_spread 0 --res_mean $mean --res_het 0 --res_dyn $spread --conc $conc --gens $gens --trials $trials
					wait
					sleep $sleep_dur
					echo "$dest: Finished case-$c: $mean $spread $conc $gens" >> $log_path/progress.log
				done
			done
		done
	done
	cd ../../
done


# # Res Het exps
# for c in `seq 1 4`
# do
# 	cd $dest/case-$c
# 	cp ../../runme_$dest.py .
# 	for mean in $mean_range
# 	do
# 		for spread in $spread_range
# 		do
# 			for conc in $conc_range
# 			do
# 				for gens in $gens_range
# 				do
# 					echo "$dest: Started case-$c: $mean $spread $conc $gens" >> $log_path/progress.log
# 					python runme_$dest.py --wl_mean 1024 --wl_spread 0 --res_mean $mean --res_het $spread --res_dyn 0 --conc $conc --gens $gens --trials $trials
# 					wait
# 					sleep $sleep_dur
# 					echo "$dest: Finished case-$c: $mean $spread $conc $gens" >> $log_path/progress.log
# 				done
# 			done
# 		done
# 	done
# 	cd ../../
# done


# WL Het exps
# for c in `seq 4 4`
# do
# 	cd $dest/case-$c
# 	cp ../../runme_$dest.py .
# 	for mean in $mean_range
# 	do
# 		for spread in $spread_range
# 		do
# 			for conc in $conc_range
# 			do
# 				for gens in $gens_range
# 				do
# 					echo "Started case-$c: $mean $spread $conc $gens" >> $log_path/progress.log
# 					python runme_$dest.py --wl_mean $mean --wl_spread $spread --res_mean 32 --res_het 0 --res_dyn 0 --conc $conc --gens $gens --trials $trials
# 					wait
# 					sleep $sleep_dur
# 					echo "Finished case-$c: $mean $spread $conc $gens" >> $log_path/progress.log
# 				done
# 			done
# 		done
# 	done
# 	cd ../../
# done

