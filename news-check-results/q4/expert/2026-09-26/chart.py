import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Data: label -> (pct_decline, n, note)
metrics = [
    ("NIH total extramural\nawards, FY25 vs FY24", 6.2, "n=55,394 (FY25) vs 59,053 (FY24)"),
    ("NIH RPG new/competing\nawards, FY25 vs FY24", 20.5, "n=8,161 (FY25) vs 10,265 (FY24)"),
    ("NIH R01-equiv. new/competing\nawards, FY25 vs FY24", 21.8, "n=5,471 (FY25) vs 7,000 (FY24)"),
    ("NIH new/competitive awards,\nFY26 YTD (Mar 3) vs '21-'24 avg", 74, "partial FY26 pace, n not disclosed"),
    ("NSF new grants,\nFY26 (proj.) vs '21-'24 avg", 46, "proj. ~6,100 new grants FY26"),
]

labels = [m[0] for m in metrics]
values = [m[1] for m in metrics]
colors = ["#4C72B0", "#4C72B0", "#4C72B0", "#DD8452", "#55A868"]

fig, ax = plt.subplots(figsize=(10, 6.2))
bars = ax.barh(labels, values, color=colors)
ax.invert_yaxis()
ax.set_xlabel("Percent decline (%)")
ax.set_title("Federal research grant-count declines: metric matters\n(NIH figures are FY25 full-year vs FY24; NSF/late-NIH figures are partial-FY26 or projected)")
ax.set_xlim(0, 85)

for bar, (label, val, note) in zip(bars, metrics):
    ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
             f"{val}%  ({note})", va="center", fontsize=8.5)

plt.tight_layout()
plt.savefig("agent-dashboard.png", dpi=150)
print("saved")
