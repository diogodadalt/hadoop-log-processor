set xlabel "Time(s)"
set ylabel "Tasks"
set autoscale
set offset 0,0,1.0,0
set terminal png
set output "imagem.png"
plot "maps.out" title "Map" with lines, "shuffles.out" title "Shuffle" with lines, "reduces.out" title "Reduce" with lines