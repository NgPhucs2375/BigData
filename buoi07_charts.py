# Buổi 7c — Vẽ biểu đồ PNG cho báo cáo Word + slide thuyết trình
# Chạy (máy host):  python buoi07_charts.py
# Kết quả: các file PNG trong thư mục BigData/charts/
import json
import os
from collections import Counter

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "charts")
os.makedirs(OUT, exist_ok=True)

plt.rcParams["font.family"] = "DejaVu Sans"  # hỗ trợ tiếng Việt
plt.rcParams["axes.spines.top"] = False
plt.rcParams["axes.spines.right"] = False

ACCENT = "#a9553d"   # màu "rust" của MÙA
INK = "#202124"
SOFT = "#9b978d"


def chart_rating_distribution():
    """Phân phối điểm rating của 748K ratings mô phỏng (đọc từ data/ratings.jsonl)."""
    counts = Counter()
    with open(os.path.join(BASE, "data", "ratings.jsonl"), encoding="utf-8") as f:
        for line in f:
            counts[int(json.loads(line)["rating"])] += 1
    xs = sorted(counts)
    ys = [counts[x] for x in xs]

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar([str(x) + " sao" for x in xs], ys, color=ACCENT)
    ax.bar_label(bars, fmt="{:,.0f}", fontsize=9)
    ax.set_title("Phân phối điểm đánh giá — 748.355 ratings (bộ dữ liệu mô phỏng MÙA)")
    ax.set_ylabel("Số lượt rating")
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:,.0f}")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "chart_rating_distribution.png"), dpi=150)
    plt.close(fig)
    print("✓ chart_rating_distribution.png")


def chart_model_comparison():
    """So sánh RMSE/MAE của baseline và 3 cấu hình ALS (số liệu Buổi 5)."""
    models = ["Baseline\n(item mean)", "ALS\nrank=5", "ALS\nrank=10", "ALS\nrank=20"]
    rmse = [0.7255, 0.5525, 0.5604, 0.5594]
    mae = [0.5862, 0.4546, 0.4611, 0.4604]

    x = range(len(models))
    w = 0.38
    fig, ax = plt.subplots(figsize=(8, 4.2))
    b1 = ax.bar([i - w / 2 for i in x], rmse, w, label="RMSE", color=ACCENT)
    b2 = ax.bar([i + w / 2 for i in x], mae, w, label="MAE", color=SOFT)
    ax.bar_label(b1, fmt="%.4f", fontsize=9)
    ax.bar_label(b2, fmt="%.4f", fontsize=9)
    ax.set_xticks(list(x), models)
    ax.set_title("RMSE / MAE — ALS so với baseline (thấp hơn = tốt hơn)")
    ax.set_ylim(0, 0.85)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "chart_model_comparison.png"), dpi=150)
    plt.close(fig)
    print("✓ chart_model_comparison.png")


def chart_ranking_metrics():
    """Precision@10 / Recall@10 — ALS vs baseline (số liệu Buổi 7a)."""
    labels = ["Precision@10", "Recall@10"]
    baseline = [0.0674, 0.0333]
    als = [0.2855, 0.1406]

    x = range(len(labels))
    w = 0.38
    fig, ax = plt.subplots(figsize=(7, 4.2))
    b1 = ax.bar([i - w / 2 for i in x], baseline, w, label="Baseline top-popular", color=SOFT)
    b2 = ax.bar([i + w / 2 for i in x], als, w, label="ALS (rank=5)", color=ACCENT)
    ax.bar_label(b1, fmt="%.4f", fontsize=9)
    ax.bar_label(b2, fmt="%.4f", fontsize=9)
    ax.set_xticks(list(x), labels)
    ax.set_title("Chất lượng danh sách gợi ý — ALS gấp ~4.2 lần baseline")
    ax.set_ylim(0, 0.34)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "chart_ranking_metrics.png"), dpi=150)
    plt.close(fig)
    print("✓ chart_ranking_metrics.png")


def chart_scale():
    """Thí nghiệm scale — đọc số liệu từ data/scale_results.json (Buổi 7b)."""
    path = os.path.join(BASE, "data", "scale_results.json")
    if not os.path.exists(path):
        print("– bỏ qua chart_scale.png (chưa có data/scale_results.json)")
        return
    data = json.load(open(path, encoding="utf-8"))
    sizes = [d["size"] for d in data]
    times = [d["fit_seconds"] for d in data]

    fig, ax = plt.subplots(figsize=(7, 4.2))
    bars = ax.bar([f"{s/1000:,.0f}K" for s in sizes], times, color=ACCENT)
    ax.bar_label(bars, fmt="%.1fs", fontsize=9)
    ax.set_title("Thời gian huấn luyện ALS theo kích thước dữ liệu (rank=5, maxIter=10)")
    ax.set_xlabel("Số ratings")
    ax.set_ylabel("Thời gian fit (giây)")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "chart_scale.png"), dpi=150)
    plt.close(fig)
    print("✓ chart_scale.png")


if __name__ == "__main__":
    chart_rating_distribution()
    chart_model_comparison()
    chart_ranking_metrics()
    chart_scale()
    print(f"\nBiểu đồ nằm tại: {OUT}")
