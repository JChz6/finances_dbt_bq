// Solo líneas: sombra pequeña; las barras, etiquetas y leyendas permanecen nítidas.
Chart.register({
 id:'dashboardLineLight',
 beforeDatasetsUpdate(chart) {
  if (!chart.chartArea) return;
  const {top,bottom}=chart.chartArea;
  for (const dataset of chart.data.datasets) {
   if ((dataset.type || chart.config.type)==='line' && dataset.label==='Balance Acumulado') {
    const gradient=chart.ctx.createLinearGradient(0,top,0,bottom);
    gradient.addColorStop(0,'rgba(74,222,128,0.22)');
    gradient.addColorStop(1,'rgba(74,222,128,0)');
    dataset.backgroundColor=gradient;
    dataset.borderColor='#4ade80';
   }
  }
 },
 beforeDatasetDraw(chart,args) {
  chart.ctx.save();
  const dataset=chart.data.datasets[args.index];
  if ((dataset.type || chart.config.type)==='line') {
   const color=typeof dataset.borderColor==='string' ? dataset.borderColor : '#4ade80';
   const dailyCashFlow=chart.canvas.id==='flujoChart' && globalGranularidad==='diario';
   const alpha=dailyCashFlow ? '26' : 'bf';
   chart.ctx.shadowColor=/^#[0-9a-f]{6}$/i.test(color) ? color+alpha : `rgba(74,222,128,${dailyCashFlow ? 0.15 : 0.75})`;
   chart.ctx.shadowBlur=dailyCashFlow ? 1 : 4;
  }
 },
 afterDatasetDraw(chart) { chart.ctx.restore(); }
});
