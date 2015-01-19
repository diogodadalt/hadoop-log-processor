set xlabel "Time(s)"
set ylabel "Tasks"
set terminal png
set output "imagem.png"
plot "maps.out" title "Map" with lines, "shuffles.out" title "Shuffle" with lines, "reduces.out" title "Reduce" with lines
