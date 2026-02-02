# Analyzing Energy Risk in a Changing Winter Environment

**Location**: MISO North with emphasis on Minnesota
**Data Sources**: MISO Pricing API, MISO Generation API, MISO FTR Annual Auction Summary, MISO RTDIP, KMSP Weather Station

---

## Executive Summary

Utilities across the Midwest have accelerated their transition to variable renewable energy (VRE) sources, particularly wind, as part of climate change efforts. This transformation brings complexity as utilities must balance grid reliability, market economics, and environmental goals while maintaining acceptable costs for ratepayers. This project looks especially through the lens of Minnesota co-op Great River Energy, which generates 44% of its power from wind.

As generation mix shifts toward VRE resources, understanding how climate variability translates into market risk becomes critical. Minnesota winters are changing. They are warming on average, but also exhibiting increasingly severe cold extremes that stress both renewable and traditional generation. These tail events create price spikes that challenge risk management frameworks.

This analysis examines standard financial hedging instruments-Auction Revenue Rights (ARRs) and Financial Transmission Rights (FTRs)-and assesses whether they sufficiently protect utilities in the evolving landscape. The project has four stages: 

1. Documenting Minnesota's changing winter weather patterns and the impact on generation.
2. Demonstrating how extreme weather events translate into extreme market outcomes through case studies of Winter Storm Uri (2021) and the most recent January 2026 cold snap. 
3. Evaluating how ARRs and FTRs function under normal versus extreme market conditions. 
4. Assessing the limitations of conventional risk management tools and identifying what remains unhedged.

The goal is not to prescribe a specific strategy—that requires understanding Great River Energy's unique risk tolerance, regulatory framework, and stakeholder priorities. Rather, this project serves to inform on risk. It aims to analyze what current tools actually do versus what we assume they protect against, particularly during the extreme winter events where losses concentrate.

---

## Part 1: Understanding the Evolving Winter Energy Landscape

### Minnesota's Changing Winter Climate

Climate change is most often described through the lens of mean temperature rising. We observed that to be true comparing the last eight (2018-2026) Minnesota winters to the previous eight (2009-2017). However the average temperature fails to convey what is happening at the tails of the distribtion, which is critical from an energy perspective. Skewness is used to measure the tail of a distribution. Negative skewness indicates more cold weather events. This analysis shows that skewness has become pronouncly more negative over recent years. This leads to the counterintuitve realization that while average temperature is rising, extreme cold snaps are actually becoming more severe.

<img src="plots/winter_temperature_histogram.png" width="75%" style="display: block; margin: 0 auto;" />

At the same time winter wind patterns are also changing. In this case average wind speed is decreasing. Skewness increased in the opposite direction meaning more severe high winds, but more importantly for energy providers an increased frequency of low wind hours.

<img src="plots/winter_winds_histogram.png" width="75%" style="display: block; margin: 0 auto;" />

The data clearly points to two distinct and important trends in Minnesota's winter climate: First, the average temperature is warming but with increased variability and severity of extreme cold. Second, average wind is decreasing with more total low wind hours. This combination can have an outsized impact on energy prices as we will examine next. 

To understand how weather affects market risk, it's important to briefly explain how electricity prices work in MISO. The grid operator runs a market where generators offer to supply power at various prices. The lowest-cost resources (wind, solar, nuclear, coal baseload) are dispatched first, with more expensive generators called upon only when demand is high or when cheaper resources are unavailable.

Electricity prices are determined by locational marginal pricing (LMP) - the cost of serving one additional megawatt (MW) of demand at a given location on the grid. During normal conditions, LMPs reflect routine congestion and fuel costs. During extreme events, prices can spike if infrastructure freezes, natural gas gets diverted to heating, and transmission congestion occurs and power can't flow to where it's needed.

Wind generation plays a critical role here. During normal winter conditions, abundant wind keeps prices low by reducing reliance on gas-fired generation. But during extreme cold events, if wind output drops when there is high heating demand and gas generator outages, prices can spike dramatically.

The plot below illustrates this relationship. Each point represents an hour at a node, with temperature on the X-axis and real-time electricity prices on the Y-axis. Points are colored by wind generation levels - blue dots mean less wind generation is occuring and red dots are high generation. The clustering of extreme prices (>$1,000/MWh) occurs when very cold temperatures coincide with low wind generation.

<img src="plots/rt_lmp_vs_temperature_windmw_rt.png" width="75%" style="display: block; margin: 0 auto;" />







