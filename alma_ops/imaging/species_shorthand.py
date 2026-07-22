"""
species_shorthand.py
----------------------
Clean chemical-formula shorthand for ADMIT's full species names.

ADMIT (via its Splatalogue-derived species.json) reports each detection with
both a descriptive name (species_name, e.g. "Formaldehyde") and a formula
string (e.g. "H2CO", but also "CH3OHvt=0" or "c-HCCCHv=0" for species with
isotopologue/vibrational-state tags) -- accurate, but not always what's
fastest to scan by eye. This is a hand-curated map from species_name to a
clean shorthand, extended as new species turn up in ADMIT line-ID runs.

Some species require frequency-aware lookup because ADMIT groups isotopologues
under the parent species name. The canonical examples:
  - "Carbon Monoxide" is used for CO, 13CO, C18O, and C17O
  - "Formylium" covers HCO+ and its isotopologues (DCO+, HC18O+)
  - "Carbon Monosulfide" covers CS, 13CS, C34S
  - "Hydrogen Isocyanide" covers HNC and DNC
  - "Cyanogen" covers CN and 13CN
_TRANSITION_RANGES resolves these to specific shorthands when a detection
frequency is provided.

Rest frequencies drawn from PHANGS utilsLines.py v3.2 (Leroy et al. 2021,
https://github.com/PhangsTeam/phangs_imaging_scripts) supplemented with
manually added values for protostellar tracers not covered there (DCO+, DNC,
HC3N Band 6 transitions, HCO+ isotopologues). No runtime dependency on PHANGS.
"""

from __future__ import annotations

# Species-level shorthands: used when no frequency is supplied, or as
# fallback when a detection frequency matches no entry in _TRANSITION_RANGES.
# ADMIT uses the same species_name for all isotopologues of a molecule, so the
# per-isotopologue label is resolved via _TRANSITION_RANGES when available.
SPECIES_SHORTHAND: dict[str, str] = {
    # --- confirmed species names from actual ADMIT output in this survey ---
    "Carbon Monoxide": "CO",         # also 13CO, C18O, C17O — see _TRANSITION_RANGES
    "Carbon Monosulfide": "CS",      # also 13CS, C34S — see _TRANSITION_RANGES
    "Cyanogen": "CN",                # also 13CN
    "Formaldehyde": "H2CO",          # also HDCO, H2-13CO
    "Formylium": "HCO+",             # also DCO+, HC18O+ — see _TRANSITION_RANGES
    "Hydrogen Cyanide": "HCN",
    "Hydrogen Isocyanide": "HNC",    # also DNC — see _TRANSITION_RANGES
    "Silicon Monoxide": "SiO",       # transition-specific via _TRANSITION_RANGES
    "Sulfur Monoxide": "SO",
    "Sulfur Dioxide": "SO2",         # uppercase D — ADMIT uses for isotopologues
    "Sulfur dioxide": "SO2",         # lowercase d — ADMIT uses for main SO2 species
    "Thioformaldehyde": "H2CS",
    "Diazenylium": "N2H+",
    "Cyanoacetylene": "HC3N",        # transition-specific via _TRANSITION_RANGES
    "Cyclopropenylidene": "c-C3H2",
    "Atomic Carbon": "C",
    "Isocyanic acid": "HNCO",
    "Carbonyl Sulfide": "OCS",
    "Ketene": "H2CCO",
    "Methanimine": "CH2NH",
    "Hydrogen sulfide": "H2S",
    "Methyl Cyanide": "CH3CN",
    "Methyl Acetylene": "CH3CCH",
    # --- complex organics (hot corino tracers) ---
    "Dimethyl ether": "CH3OCH3",
    "Ethylene Glycol": "(CH2OH)2",
    "Formic Acid": "HCOOH",
    "Methanol": "CH3OH",
    "Methyl Formate": "HCOOCH3",
    "Vinyl Cyanide": "CH2CHCN",
    "Ethyl Cyanide": "CH3CH2CN",
    "gauche-Ethanol": "C2H5OH",
    "Acetaldehyde": "CH3CHO",
    "Propanal": "CH3CH2CHO",
    "Glycolaldehyde": "HOCH2CHO",
    "Cyanoallene": "H2CCCHCN",
    "2-Propynal": "HCCCHO",
    "Acetic Acid": "CH3COOH",
    "Methylamine": "CH3NH2",
    "Aminoacetonitrile": "H2NCH2CN",
}

# Frequency-aware transition table: (species_name, freq_lo_ghz, freq_hi_ghz, shorthand).
# Checked before SPECIES_SHORTHAND when a frequency is provided. Windows are
# set by bisecting the gap to the nearest same-species transition so that no
# two entries for the same species overlap.
_TRANSITION_RANGES: list[tuple[str, float, float, str]] = [

    # ── Carbon Monoxide (ADMIT uses "Carbon Monoxide" for all isotopologues) ──
    # Band 3 — windows split at midpoints between adjacent isotopologue rest freqs
    ("Carbon Monoxide", 109.30, 109.99, "C18O(1-0)"),   # rest 109.782 GHz
    ("Carbon Monoxide", 109.99, 110.65, "13CO(1-0)"),   # rest 110.201 GHz
    ("Carbon Monoxide", 111.90, 112.85, "C17O(1-0)"),   # rest 112.359 GHz
    ("Carbon Monoxide", 114.80, 115.75, "CO(1-0)"),     # rest 115.271 GHz
    # Band 6
    ("Carbon Monoxide", 219.10, 219.97, "C18O(2-1)"),   # rest 219.560 GHz
    ("Carbon Monoxide", 219.97, 220.85, "13CO(2-1)"),   # rest 220.399 GHz
    ("Carbon Monoxide", 224.20, 225.20, "C17O(2-1)"),   # rest 224.714 GHz
    ("Carbon Monoxide", 230.00, 231.10, "CO(2-1)"),     # rest 230.538 GHz
    # Band 7
    ("Carbon Monoxide", 328.85, 329.96, "C18O(3-2)"),   # rest 329.331 GHz
    ("Carbon Monoxide", 329.96, 331.10, "13CO(3-2)"),   # rest 330.588 GHz
    ("Carbon Monoxide", 345.30, 346.30, "CO(3-2)"),     # rest 345.796 GHz

    # ── Silicon Monoxide ──────────────────────────────────────────────────────
    ("Silicon Monoxide",  86.30,  87.30, "SiO(2-1)"),   # rest  86.847 GHz  Band 3
    ("Silicon Monoxide", 173.20, 174.20, "SiO(4-3)"),   # rest 173.688 GHz  Band 4
    ("Silicon Monoxide", 216.60, 217.60, "SiO(5-4)"),   # rest 217.105 GHz  Band 6
    ("Silicon Monoxide", 260.00, 261.00, "SiO(6-5)"),   # rest 260.518 GHz  Band 6
    ("Silicon Monoxide", 303.40, 304.40, "SiO(7-6)"),   # rest 303.927 GHz  Band 7

    # ── Formylium (ADMIT uses "Formylium" for HCO+ isotopologue family) ──────
    # DCO+(3-2) at 216.113 GHz and HC18O+(2-1) at 170.323 GHz appear as "Formylium"
    ("Formylium",  88.70,  89.65, "HCO+(1-0)"),         # rest  89.189 GHz  Band 3
    ("Formylium", 169.85, 170.80, "HC18O+(2-1)"),       # rest 170.323 GHz  Band 4
    ("Formylium", 177.90, 178.90, "HCO+(2-1)"),         # rest 178.375 GHz  Band 4
    ("Formylium", 215.60, 216.60, "DCO+(3-2)"),         # rest 216.113 GHz  Band 6
    ("Formylium", 259.75, 260.75, "H13CO+(3-2)"),       # rest 260.255 GHz  Band 6
    ("Formylium", 267.10, 268.10, "HCO+(3-2)"),         # rest 267.558 GHz  Band 6
    ("Formylium", 356.20, 357.20, "HCO+(4-3)"),         # rest 356.734 GHz  Band 7

    # ── Hydrogen Cyanide ──────────────────────────────────────────────────────
    ("Hydrogen Cyanide",  88.10,  89.15, "HCN(1-0)"),   # rest  88.632 GHz  Band 3
    ("Hydrogen Cyanide", 265.40, 266.40, "HCN(3-2)"),   # rest 265.886 GHz  Band 6
    ("Hydrogen Cyanide", 354.00, 355.10, "HCN(4-3)"),   # rest 354.505 GHz  Band 7

    # ── Hydrogen Isocyanide (also DNC; all "Hydrogen Isocyanide" in ADMIT) ───
    ("Hydrogen Isocyanide",  90.20,  91.20, "HNC(1-0)"),  # rest  90.664 GHz  Band 3
    ("Hydrogen Isocyanide", 228.40, 229.40, "DNC(3-2)"),  # rest 228.910 GHz  Band 6
    ("Hydrogen Isocyanide", 271.50, 272.50, "HNC(3-2)"),  # rest 271.981 GHz  Band 6

    # ── Carbon Monosulfide (all isotopologues share "Carbon Monosulfide") ────
    ("Carbon Monosulfide",  97.50,  98.50, "CS(2-1)"),   # rest  97.981 GHz  Band 3
    ("Carbon Monosulfide", 146.50, 147.50, "CS(3-2)"),   # rest 146.969 GHz  Band 4
    ("Carbon Monosulfide", 192.35, 193.35, "C34S(4-3)"), # rest 192.819 GHz  Band 4
    ("Carbon Monosulfide", 195.50, 196.50, "CS(4-3)"),   # rest 195.954 GHz  Band 4
    ("Carbon Monosulfide", 230.75, 231.75, "13CS(5-4)"), # rest 231.221 GHz  Band 6
    ("Carbon Monosulfide", 244.45, 245.45, "CS(5-4)"),   # rest 244.936 GHz  Band 6
    ("Carbon Monosulfide", 293.00, 294.00, "CS(6-5)"),   # rest 293.912 GHz  Band 7
    ("Carbon Monosulfide", 342.40, 343.40, "CS(7-6)"),   # rest 342.883 GHz  Band 7

    # ── Cyanogen (CN; all "Cyanogen" in ADMIT) ────────────────────────────────
    # CN(1-0) spans two hyperfine groups at ~113.2 and ~113.5 GHz
    ("Cyanogen", 112.70, 114.10, "CN(1-0)"),             # rest 113.191/113.491 GHz  Band 3

    # ── Diazenylium (N2H+) ────────────────────────────────────────────────────
    ("Diazenylium",  92.70,  93.70, "N2H+(1-0)"),        # rest  93.173 GHz  Band 3
    ("Diazenylium", 279.00, 280.00, "N2H+(3-2)"),        # rest 279.512 GHz  Band 7

    # ── Cyanoacetylene (HC3N) ─────────────────────────────────────────────────
    ("Cyanoacetylene", 108.70, 109.70, "HC3N(12-11)"),   # rest 109.174 GHz  Band 3
    ("Cyanoacetylene", 217.85, 218.85, "HC3N(24-23)"),   # rest 218.325 GHz  Band 6
    ("Cyanoacetylene", 226.90, 227.90, "HC3N(25-24)"),   # rest 227.419 GHz  Band 6
    ("Cyanoacetylene", 236.00, 237.00, "HC3N(26-25)"),   # rest 236.513 GHz  Band 6
]


def shorthand_for(species_name: str | None, frequency_ghz: float | None = None) -> str | None:
    """Clean chemical shorthand for a species_name, or None if not yet curated.

    When frequency_ghz is provided, checks _TRANSITION_RANGES first so that
    isotopologue families (e.g. "Carbon Monoxide" → CO vs 13CO vs C18O, or
    "Formylium" → HCO+ vs DCO+) resolve to the correct transition-specific label.
    """
    if species_name is None:
        return None
    if frequency_ghz is not None:
        for name, lo, hi, label in _TRANSITION_RANGES:
            if name == species_name and lo <= frequency_ghz <= hi:
                return label
    return SPECIES_SHORTHAND.get(species_name)


# ---------------------------------------------------------------------------
# TODO: species/transitions still to be added
#
# These are harder protostellar tracers not covered by PHANGS utilsLines.py.
# Look up rest frequencies in Splatalogue (splatalogue.net) and add entries
# to _TRANSITION_RANGES (if the species appears at multiple transitions that
# need disambiguation) or SPECIES_SHORTHAND (if a single label is sufficient).
#
# Sulfur Monoxide (SO) — "Sulfur Monoxide" confirmed in DB at 172.181 and
#   340.714 GHz (Bands 4/7). Common Band 6 lines: SO(5₆-4₅) ~219.949 GHz,
#   SO(6₅-5₄) ~251.826 GHz. Multiple transitions → needs _TRANSITION_RANGES.
#
# Formaldehyde isotopologues — "Formaldehyde" covers H2CO, HDCO, H2¹³CO.
#   H2CO(3₀₃-2₀₂) at 218.222 GHz (Band 6) is very common in hot corinos.
#   HDCO and H2¹³CO appear at distinct frequencies → add _TRANSITION_RANGES
#   entries to disambiguate (e.g. HDCO(4₁₄-3₁₃) ~246.924 GHz).
#
# Isocyanic acid (HNCO) — "Isocyanic acid" in SPECIES_SHORTHAND. Common
#   Band 3: HNCO(4₀₄-3₀₃) ~87.925 GHz. Band 6: HNCO(10₀₁₀-9₀₉) ~219.798 GHz.
#   Add _TRANSITION_RANGES if needed to distinguish J-ladders.
#
# Ethynyl radical (CCH) — common Band 3 tracer with hyperfine components
#   near ~87.3 GHz. Confirm ADMIT's species_name from a real detection before
#   adding (Splatalogue name may differ from what ADMIT reports).
#
# Cyclopropenylidene (c-C3H2) — "Cyclopropenylidene" in SPECIES_SHORTHAND.
#   Band 3 transitions: 2₁₂-1₀₁ at ~85.338 GHz, others near 150 GHz.
#   Add _TRANSITION_RANGES if multiple transitions show up in the same survey.
#
# Carbonyl Sulfide (OCS) — "Carbonyl Sulfide" in SPECIES_SHORTHAND.
#   Band 7 transition at ~340.449 GHz (OCS(28-27)). Confirm species_name.
#
# Complex organics with many transitions (CH3OH, CH3OCH3, HCOOCH3, CH3CHO,
#   CH3CH2CN, etc.) — already in SPECIES_SHORTHAND. These molecules have
#   hundreds of transitions across Bands 3/6/7; adding per-transition entries
#   is impractical. Species-level shorthands are likely sufficient unless ADMIT
#   starts misidentifying them across families, in which case frequency ranges
#   for the most common "anchor" lines would help.
#
# HC3N vibrational states (v7=1, v7=2 hot lines) — appear in hot corinos
#   but may be labeled differently by ADMIT than ground-state "Cyanoacetylene".
#   Confirm species_name and add _TRANSITION_RANGES once observed.
# ---------------------------------------------------------------------------


def species_label(
    species_name: str | None,
    formula: str | None = None,
    frequency_ghz: float | None = None,
) -> str:
    """Display label combining the full species name with its shorthand, e.g.
    "Formaldehyde (H2CO)". Falls back to the raw ADMIT formula when no curated
    shorthand exists yet, and to just the name/formula alone when neither
    differs or exists."""
    name = species_name or formula or ""
    short = shorthand_for(species_name, frequency_ghz) or formula
    if short and short != name:
        return f"{name} ({short})"
    return name