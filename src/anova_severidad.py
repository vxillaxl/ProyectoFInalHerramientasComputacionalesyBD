# ANOVA de una via: severidad del accidente segun la franja horaria.
# Los suficientes estadisticos salen del ETL (n, media, varianza de toda la base).
# Shapiro-Wilk y Levene se calculan sobre la muestra aleatoria, porque esas
# pruebas no estan hechas para millones de filas y Shapiro-Wilk admite hasta 5.000.

import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from scipy import stats

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RESULTADOS = os.path.join(ROOT, "resultados")

ORDEN = ["Madrugada", "Manana", "Tarde", "Noche"]
ETIQUETA = {
    "Madrugada": "Madrugada (0-5)",
    "Manana": "Manana (6-11)",
    "Tarde": "Tarde (12-17)",
    "Noche": "Noche (18-23)",
}


def interpretar_eta(eta):
    if eta < 0.01:
        return "insignificante"
    if eta < 0.06:
        return "pequeño"
    if eta < 0.14:
        return "mediano"
    return "grande"


def main():
    grupos = pd.read_csv(os.path.join(RESULTADOS, "anova_por_franja.csv"))
    grupos["orden"] = grupos["franja"].map({nombre: i for i, nombre in enumerate(ORDEN)})
    grupos = grupos.sort_values("orden")

    n_total = int(grupos["n"].sum())
    k = int(len(grupos))
    media_global = float((grupos["n"] * grupos["media"]).sum() / n_total)
    ssb = float((grupos["n"] * (grupos["media"] - media_global) ** 2).sum())
    ssw = float(((grupos["n"] - 1) * grupos["varianza"]).sum())
    dfb = k - 1
    dfw = n_total - k
    msb = ssb / dfb
    msw = ssw / dfw
    f_stat = msb / msw
    p_valor = float(stats.f.sf(f_stat, dfb, dfw))
    eta = ssb / (ssb + ssw)
    rechaza = bool(p_valor < 0.05)

    muestra = pd.read_csv(os.path.join(RESULTADOS, "anova_muestra.csv"))
    shapiro = []
    listas = []
    for franja in ORDEN:
        valores = muestra.loc[muestra["franja"] == franja, "severidad"].astype(float)
        listas.append(valores.to_numpy())
        toma = valores.sample(n=min(5000, len(valores)), random_state=42)
        w_stat, w_p = stats.shapiro(toma)
        shapiro.append({
            "franja": franja,
            "n_shapiro": int(len(toma)),
            "W": round(float(w_stat), 4),
            "p": float(w_p),
        })

    levene_w, levene_p = stats.levene(*listas, center="median")

    if rechaza:
        decision = (
            "Se rechaza la hipotesis nula: la severidad media no es la misma "
            "en las cuatro franjas horarias."
        )
    else:
        decision = (
            "No se rechaza la hipotesis nula: no hay evidencia de que la severidad "
            "media cambie entre las franjas horarias."
        )

    efecto = interpretar_eta(eta)
    conclusion = (
        f"{decision} F({dfb}, {dfw}) = {f_stat:.2f}, p = {p_valor:.4g}, "
        f"eta cuadrado = {eta:.4f} (efecto {efecto}). "
        "La severidad es una escala discreta de 1 a 4, asi que el supuesto de "
        "normalidad no se cumple y Shapiro-Wilk lo confirma en la muestra. "
        "Con millones de observaciones el ANOVA detecta diferencias muy pequenas; "
        "por eso la lectura practica se apoya en el tamano del efecto y en las medias, "
        "no solo en el p-valor."
    )

    salida = {
        "variable_dependiente": "severity (1 a 4)",
        "factor": "franja horaria",
        "hipotesis_nula": "La severidad media es igual en madrugada, manana, tarde y noche.",
        "hipotesis_alternativa": "Al menos una franja tiene una severidad media distinta.",
        "alpha": 0.05,
        "n": n_total,
        "k": k,
        "media_global": round(media_global, 4),
        "F": round(f_stat, 4),
        "p": p_valor,
        "df_entre": dfb,
        "df_dentro": dfw,
        "eta_cuadrado": round(eta, 6),
        "tamano_efecto": efecto,
        "rechaza_h0": rechaza,
        "shapiro": shapiro,
        "levene_W": round(float(levene_w), 4),
        "levene_p": float(levene_p),
        "grupos": grupos.to_dict(orient="records"),
        "conclusion": conclusion,
    }

    destino = os.path.join(RESULTADOS, "anova_resultado.json")
    with open(destino, "w", encoding="utf-8") as archivo:
        json.dump(salida, archivo, ensure_ascii=False, indent=2)

    graficar(grupos, muestra)
    print(conclusion)
    print(f"[OK] {destino}")


def graficar(grupos, muestra):
    etiquetas = [ETIQUETA[f] for f in grupos["franja"]]
    medias = grupos["media"].tolist()
    error = (grupos["varianza"] / grupos["n"]).pow(0.5)

    fig, ax = plt.subplots(figsize=(8.2, 4.6))
    barras = ax.bar(etiquetas, medias, color="#1f4e79", yerr=error, capsize=4, ecolor="#333333")
    ax.set_ylabel("Severidad media")
    ax.set_ylim(1, max(4, max(medias) + 0.4))
    ax.set_title("Severidad media segun la franja del dia")
    for barra, media in zip(barras, medias):
        ax.text(barra.get_x() + barra.get_width() / 2, media + 0.05, f"{media:.3f}", ha="center", va="bottom", fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTADOS, "anova_franjas.png"), dpi=140)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.2, 4.6))
    datos = [muestra.loc[muestra["franja"] == franja, "severidad"].astype(float) for franja in ORDEN]
    ax.boxplot(datos, showfliers=False)
    ax.set_xticklabels([ETIQUETA[f] for f in ORDEN])
    ax.set_ylabel("Severidad")
    ax.set_title("Distribucion de la severidad por franja (muestra, sin atipicos)")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTADOS, "anova_boxplot.png"), dpi=140)
    plt.close(fig)


if __name__ == "__main__":
    main()
