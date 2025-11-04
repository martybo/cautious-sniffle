#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ---------- tiny utils ----------
def now() -> str: return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def log(msg: str): print(f"[{now()}] {msg}", flush=True)
def slug(s): return re.sub(r"[\s\-_]+"," ",str(s).strip().lower()).replace(" ","")

def find_col(df, candidates):
    slugs = {c: slug(c) for c in df.columns}
    inv = {v:k for k,v in slugs.items()}
    for cand in candidates:
        sc = slug(cand)
        if sc in inv: return inv[sc]
    for cand in candidates:
        sc = slug(cand)
        if len(sc)>=4:
            for c,sc2 in slugs.items():
                if sc in sc2: return c
    return None

def ensure_numeric(s):
    out = pd.to_numeric(s, errors="coerce")
    return out.fillna(0)

def parse_date(s): return pd.to_datetime(s, errors="coerce", dayfirst=True)

def coalesce(a, b):
    a = a.astype("string"); b = b.astype("string")
    pick_a = a.str.strip().ne("") & a.notna()
    return a.where(pick_a, b)

def dns_present(series: pd.Series) -> pd.Series:
    s = series.astype("string")
    t = s.str.strip().str.lower()
    zero_like = t.str.fullmatch(r"0+(\.0+)?")
    placeholders = t.isin({"", "na", "n/a", "none", "null", "nan", "-", "--", "."})
    empty = t.isna() | placeholders | zero_like
    return ~empty

def autosize_sheet(writer, sheet_name, df):
    ws = writer.sheets[sheet_name]
    for idx, col in enumerate(df.columns):
        series = df[col].astype("string").fillna("")
        # sample up to first 1500 values for speed
        sample = series.head(1500)
        max_len = max([len(str(col))] + [len(x) for x in sample])
        ws.set_column(idx, idx, min(max_len + 2, 60))

def apply_formats(writer, sheet_name, df):
    wb = writer.book
    ws = writer.sheets[sheet_name]
    pct_fmt = wb.add_format({"num_format": "0.0%"})
    for idx, col in enumerate(df.columns):
        if str(col).lower().endswith("_pct"):
            ws.set_column(idx, idx, None, pct_fmt)

def add_share_and_rate(df, qty_col, denom_col):
    if df.empty:
        df[qty_col + "_pct"] = []
        df["shortage_rate_pct"] = []
        return df
    total_qty = df[qty_col].sum()
    df[qty_col + "_pct"] = (df[qty_col] / total_qty) if total_qty else 0.0
    df["shortage_rate_pct"] = (df[qty_col] / df[denom_col]).replace([np.inf, np.nan], 0.0)
    return df

def safe_str_norm(x):
    return str(x).strip().casefold()

# ---------- candidates ----------
CAND = {
    "pipcode":     ["PIPCode","PIP Code","PIP","productCode","product code"],
    "branch":      ["Branch Name","Branch","Store","Store Name"],
    "completed":   ["Completed Date","Completed","Booked Date","Booked"],
    "orderno":     ["Branch Order No.","Order No.","OrderNo","BranchOrderNo"],
    "department":  ["departmentName","Department"],
    "suppliername":["supplierName","Supplier Name","Supplier Name (Display)"],
    "groupname":   ["groupName","Group Name","Group"],
    "orderlist":   ["Supplier","Orderlist","Ordering Supplier","Supplier (Orderlist)"],
    "dns":         ["doNotStockReason","Do Not Stock Reason","DNS Reason"],
    "maxord":      ["maxOrderQuantity","Max Order Quantity","Max Ord Qty","Max Qty"],
    "req":         ["Store Order Quantity","Req Qty","Requested Qty","ReqQty"],
    "ord":         ["Warehouse Reply Quantity","Order Qty","Ordered Qty","Reply Qty","OrderQty"],
    "delv":        ["Store Received Quantity","Deliver Qty","Delivered Qty","Received Qty","DeliverQty"],
}

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(
        description="TRUE shortage report (v2h): unmatched PIP exclusion + tabs, subs exclusion, Rule2 caps out of denominators, reset-on-success, NC = WH+WH-CD only, alias mapping, rogue lines, masks aligned."
    )
    ap.add_argument("--orders", required=True)
    ap.add_argument("--product-list", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--subs", default=None)
    ap.add_argument("--subs-desc-col", default="Product Name")
    ap.add_argument("--subs-pack-col", default="Pack")
    ap.add_argument("--completed-only", action="store_true", help="Exclude non-completed rows (default includes with OrderQty-as-delivery).")
    ap.add_argument("--dns-source", choices=["orders","product","both"], default="orders")
    ap.add_argument("--no-reset-on-success", action="store_true")
    ap.add_argument("--warehouse-orderlists", default="Warehouse;Medicare Warehouse;Xmas Warehouse;Perfumes;Warehouse CDs;Warehouse - CD Products")
    ap.add_argument("--nc-orderlists", default="Warehouse;Warehouse - CD Products")
    ap.add_argument("--branch-alias-csv", default=None)  # columns: Source,Alias

    args = ap.parse_args()
    out_path = Path(args.out); out_path.parent.mkdir(parents=True, exist_ok=True)

    # build suffix with earliest Completed Date as wcDDMMYY
    suffix = "_wc" + datetime.now().strftime("%d%m%y")
    errlog = out_path.parent / f"run_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    try:
        log(f"Reading orders: {args.orders}")
        orders = pd.read_csv(args.orders) if str(args.orders).lower().endswith(".csv") else pd.read_excel(args.orders)
        log(f"Orders rows: {len(orders)} cols: {len(orders.columns)}")

        log(f"Reading product list: {args.product_list}")
        prod = pd.read_csv(args.product_list) if str(args.product_list).lower().endswith(".csv") else pd.read_excel(args.product_list)
        log(f"Product rows: {len(prod)} cols: {len(prod.columns)}")

        subs = None
        if args.subs:
            log(f"Reading substitutions: {args.subs}")
            subs = pd.read_csv(args.subs) if str(args.subs).lower().endswith(".csv") else pd.read_excel(args.subs)
            log(f"Substitutions rows: {len(subs)} cols: {len(subs.columns)}")

        alias_map = {}
        if args.branch_alias_csv and Path(args.branch_alias_csv).exists():
            a = pd.read_csv(args.branch_alias_csv)
            # Expect columns: Source, Alias
            src_col = find_col(a, ["Source","source","Branch","Branch Name","From"])
            ali_col = find_col(a, ["Alias","alias","To"])
            if src_col and ali_col:
                alias_map = dict(zip(a[src_col].astype(str), a[ali_col].astype(str)))

        # ---- map columns in Orders ----
        oc = {}
        for k in ["pipcode","branch","completed","orderno","department","suppliername","groupname","orderlist","dns","maxord","req","ord","delv"]:
            oc[k] = find_col(orders, CAND[k])
            log(f"Orders mapping: {k} -> {oc[k]}")

        # product key
        pc_key = find_col(prod, CAND["pipcode"])
        if not oc["pipcode"] or not pc_key:
            raise KeyError(f"Join key not found. Orders:{oc['pipcode']} Product:{pc_key}")

        # product-side refs
        prod_clean = prod.drop_duplicates(subset=[pc_key]).copy()
        prod_dep   = find_col(prod_clean, CAND["department"])
        prod_sup   = find_col(prod_clean, CAND["suppliername"])
        prod_grp   = find_col(prod_clean, CAND["groupname"])
        prod_ordl  = find_col(prod_clean, CAND["orderlist"])
        prod_name  = find_col(prod_clean, ["productName","Description","Product Description"])
        prod_pack  = find_col(prod_clean, ["packSize","Pack Size"])
        prod_bin   = find_col(prod_clean, ["binLocation","Bin Location","Bin","Bin Code"])
        prod_maxord = find_col(prod_clean, CAND["maxord"])

        # ---- merge (safe backfill only) ----
        log("Merging orders with product list...")
        df = orders.merge(prod_clean, how="left", left_on=oc["pipcode"], right_on=pc_key, suffixes=("","__prod"))

        # coalesced rollup labels
        dep_final = coalesce(df[oc["department"]] if oc["department"] else pd.Series("", index=df.index, dtype="string"),
                             df[prod_dep] if prod_dep else pd.Series("", index=df.index, dtype="string"))
        sup_final = coalesce(df[oc["suppliername"]] if oc["suppliername"] else pd.Series("", index=df.index, dtype="string"),
                             df[prod_sup] if prod_sup else pd.Series("", index=df.index, dtype="string"))
        grp_final = coalesce(df[oc["groupname"]] if oc["groupname"] else pd.Series("", index=df.index, dtype="string"),
                             df[prod_grp] if prod_grp else pd.Series("", index=df.index, dtype="string"))
        ordlist_orders = df[oc["orderlist"]].astype("string") if oc["orderlist"] else pd.Series("", index=df.index, dtype="string")
        ordlist_prod   = df[prod_ordl].astype("string") if prod_ordl else pd.Series("", index=df.index, dtype="string")
        ordlist_final  = coalesce(ordlist_orders, ordlist_prod)

        # if Group blank -> Supplier, if Supplier blank -> Department
        grp_final2 = grp_final.copy()
        grp_final2 = grp_final2.where(grp_final2.str.strip().ne(""),
                       sup_final.where(sup_final.str.strip().ne(""), dep_final))

        df["Department_Final"]   = dep_final
        df["SupplierName_Final"] = sup_final
        df["Group_Final"]        = grp_final2
        df["Orderlist_Final"]    = ordlist_final
        if prod_name: df["Product_Description"] = df[prod_name].astype("string")
        else: df["Product_Description"] = ""
        if prod_pack: df["Pack_Size"] = df[prod_pack].astype("string")
        else: df["Pack_Size"] = ""
        if prod_bin: df["Bin_Location"] = df[prod_bin].astype("string")
        else: df["Bin_Location"] = ""

        # dns final
        orders_dns  = df[oc["dns"]].astype("string") if oc["dns"] else pd.Series("", index=df.index, dtype="string")
        product_dns = df[prod_ordl]  # product doesn’t have DNS typically; keep empty if none
        product_dns = pd.Series("", index=df.index, dtype="string") if product_dns is None else product_dns.astype("string")*0
        if args.dns_source == "orders":
            df["doNotStockReason_Final"] = orders_dns
        elif args.dns_source == "product":
            df["doNotStockReason_Final"] = product_dns
        else:
            df["doNotStockReason_Final"] = orders_dns  # both -> prefer orders value

        # base numeric
        df["_Req"] = ensure_numeric(df[oc["req"]])
        df["_Ord"] = ensure_numeric(df[oc["ord"]])
        df["_Del"] = ensure_numeric(df[oc["delv"]])
        df["_Short"] = (df["_Req"] - df["_Del"]).clip(lower=0)

        # completed / effective delivery (include non-completed with Ord-as-del)
        df["_Completed"]   = parse_date(df[oc["completed"]]) if oc["completed"] else pd.NaT
        df["_IsCompleted"] = df["_Completed"].notna()
        if args.completed_only:
            metric_mask_base = df["_IsCompleted"]
            df["_EffDel"] = df["_Del"]
        else:
            metric_mask_base = pd.Series(True, index=df.index)
            df["_EffDel"] = np.where(df["_IsCompleted"], df["_Del"], df["_Ord"])
        df["_ShortEff"] = (df["_Req"] - df["_EffDel"]).clip(lower=0)

        # Branch alias (for NC)
        if alias_map and oc["branch"]:
            df["_Branch_NC"] = df[oc["branch"]].astype(str).map(lambda s: alias_map.get(s, s))
        else:
            df["_Branch_NC"] = df[oc["branch"]].astype(str) if oc["branch"] else pd.Series("", index=df.index, dtype="string")

        # ------ Substitutions (exclude) ------
        substituted_rows = 0
        if subs is not None and prod_name and prod_pack:
            s_desc = find_col(subs, [args.subs_desc_col, "Product Name","Description"])
            s_pack = find_col(subs, [args.subs_pack_col, "Pack","Pack Size"])
            if s_desc and s_pack:
                # normalise
                sub_pairs = set((safe_str_norm(a), safe_str_norm(b)) for a,b in zip(subs[s_desc], subs[s_pack]))
                # normalise df’s product+pack
                df_pairs = list(zip(df["Product_Description"].map(safe_str_norm), df["Pack_Size"].map(safe_str_norm)))
                is_sub = pd.Series([(p in sub_pairs) for p in df_pairs], index=df.index)
                substituted_rows = int(is_sub.sum())
            else:
                is_sub = pd.Series(False, index=df.index)
        else:
            is_sub = pd.Series(False, index=df.index)

        # ------ Unmatched PIPs (exclude from all calcs, but list) ------
        unmatched_mask = df[pc_key].isna()
        unmatched_pips = df.loc[unmatched_mask, oc["pipcode"]].astype(str)
        unmatched_detail = df.loc[unmatched_mask].copy()

        # ------ Rule 1 & Rule 2 (caps) ------
        has_dns_mask = dns_present(df["doNotStockReason_Final"])
        # maxorderquantity: prefer orders value else 0 (product sometimes holds this too, but in provided extracts it's not present)
        mo_candidates = []
        if oc["maxord"]:
            mo_candidates.append(oc["maxord"])
        if prod_maxord:
            mo_candidates.append(prod_maxord)
            mo_candidates.append(f"{prod_maxord}__prod")

        seen = set()
        deduped = []
        for col in mo_candidates:
            if not col or col in seen:
                continue
            seen.add(col)
            deduped.append(col)
        mo_candidates = deduped

        mo = None
        fallback_mo = None
        for col in mo_candidates:
            if col not in df.columns:
                continue
            series = ensure_numeric(df[col])
            if fallback_mo is None:
                fallback_mo = series
            if series.max(skipna=True) > 0:
                mo = series
                break
        if mo is None:
            mo = fallback_mo if fallback_mo is not None else pd.Series(0, index=df.index, dtype=float)

        # Rule 2 mask: capped if the request exceeds the maximum order quantity.
        # Allow for the common case where the warehouse supplied the full capped quantity
        # (Ord == max order).  In that situation the shortage comes from the request being
        # greater than the cap, so we look for request > cap and the order meeting the cap
        # (or higher, e.g. if the cap changes mid-stream).
        rule2_mask = (mo > 0) & (df["_Req"] > mo) & (df["_Ord"] >= mo)
        rule2_capped_lines = int(rule2_mask.sum())

        # Base metric eligibility
        metric_mask = metric_mask_base & (~unmatched_mask) & (~is_sub)

        # ------ Warehouse-like set / NC set ------
        wh_list = [x.strip().casefold() for x in args.warehouse_orderlists.split(";") if x.strip() != ""]
        nc_list = [x.strip().casefold() for x in args.nc_orderlists.split(";") if x.strip() != ""]
        ord_lower = df["Orderlist_Final"].astype("string").str.strip().str.casefold()
        wh_like_mask = ord_lower.isin(wh_list)
        nc_like_mask = ord_lower.isin(nc_list)

        warehouse_like_rows_matched = int((metric_mask & wh_like_mask).sum())

        # ------ Sort, build TrueShort with reset-on-success ------
        br = oc["branch"]; pip = oc["pipcode"]
        if not br or not pip: raise KeyError("Missing Branch or PIPCode for Rule 3.")

        df = df.sort_values(by=[br, pip, "_Completed", oc["orderno"] if oc["orderno"] else oc["req"]]).copy()

        # Reindex masks to CURRENT df after sort (this fixes the boolean-length crash):
        def reidx(s): return pd.Series(s, index=df.index).reindex(df.index, fill_value=False)

        has_dns_sorted = reidx(has_dns_mask)
        rule2_sorted   = reidx(rule2_mask)
        metric_sorted  = reidx(metric_mask)
        wh_sorted      = reidx(wh_like_mask)
        nc_sorted      = reidx(nc_like_mask)

        base_short = df["_ShortEff"].astype(float).to_numpy()
        req_qty    = df["_Req"].astype(float).to_numpy()

        # Candidate after R1/R2 exclusion: if DNS or capped -> zero
        cand = np.where(has_dns_sorted.to_numpy(), 0.0, base_short)
        # exclude capped from shortages
        cand = np.where(rule2_sorted.to_numpy(),   0.0, cand)
        # exclude rows failing metric eligibility
        cand = np.where(metric_sorted.to_numpy(),  cand, 0.0)

        reset_on_success = not args.no_reset_on_success

        inc = np.zeros(len(df), dtype=float)     # TrueShortQty for Warehouse-like rollups
        inc_all = np.zeros(len(df), dtype=float) # TrueShortQty across ALL orderlists (for Orderlist rollup)

        # denominators (dedup req window) — WH-only and ALL-routes
        denom_dep = defaultdict(float); denom_sup = defaultdict(float); denom_grp = defaultdict(float); denom_ord = defaultdict(float)
        denom_ord_all = defaultdict(float)

        state = {}  # (Branch,PIP) -> window state

        def push_window(key):
            st = state.get(key); 
            if not st: return
            w_wh  = st["window_max_req_wh"]
            w_all = st["window_max_req_all"]
            if w_wh > 0:
                if st["last_dep"] is not None: denom_dep[st["last_dep"]] += w_wh
                if st["last_sup"] is not None: denom_sup[st["last_sup"]] += w_wh
                if st["last_grp"] is not None: denom_grp[st["last_grp"]] += w_wh
                if st["last_ord"] is not None: denom_ord[st["last_ord"]] += w_wh
            if w_all > 0 and st["last_ord_all"] is not None:
                denom_ord_all[st["last_ord_all"]] += w_all
            # reset
            st["window_max_req_wh"]  = 0.0
            st["window_max_req_all"] = 0.0
            state[key] = st

        dep_final_v = df["Department_Final"].astype("string").replace("", pd.NA).to_numpy()
        sup_final_v = df["SupplierName_Final"].astype("string").replace("", pd.NA).to_numpy()
        grp_final_v = df["Group_Final"].astype("string").replace("", pd.NA).to_numpy()
        ord_final_v = df["Orderlist_Final"].astype("string").replace("", pd.NA).to_numpy()

        # Iterate in chronological order
        for i, (b, pp, c, sb, ok, rq, dval, sval, gval, oval, is_wh, is_all) in enumerate(
            zip(df[br].to_numpy(),
                df[pip].astype(str).to_numpy(),
                cand, base_short, metric_sorted.to_numpy(),
                req_qty, dep_final_v, sup_final_v, grp_final_v, ord_final_v,
                wh_sorted.to_numpy(),  # is warehouse-like?
                np.ones(len(df), dtype=bool)  # for ALL routes
            )
        ):
            key = (b, pp)
            st = state.get(key, {
                "window_max_req_wh": 0.0, "window_max_req_all": 0.0,
                "last_dep": None, "last_sup": None, "last_grp": None, "last_ord": None,
                "last_ord_all": None
            })

            # Update last-known breakdown labels
            if is_wh:
                if dval is not pd.NA: st["last_dep"] = dval
                if sval is not pd.NA: st["last_sup"] = sval
                if gval is not pd.NA: st["last_grp"] = gval
                if oval is not pd.NA: st["last_ord"] = oval
            if is_all and oval is not pd.NA:
                st["last_ord_all"] = oval

            if not ok:
                inc[i] = 0.0; inc_all[i] = 0.0
                state[key] = st
                continue

            # accumulate dedup denominators — exclude DNS and capped rows from denominators
            if is_wh and (not has_dns_sorted.iat[i]) and (not rule2_sorted.iat[i]):
                st["window_max_req_wh"] = max(st["window_max_req_wh"], float(rq))
            if is_all and (not has_dns_sorted.iat[i]) and (not rule2_sorted.iat[i]):
                st["window_max_req_all"] = max(st["window_max_req_all"], float(rq))

            # reset on success?
            if reset_on_success and sb <= 0:
                push_window(key)
                # reset running max shortage for both tracks
                if "_hist_max" not in st: st["_hist_max"] = 0.0
                if "_hist_max_all" not in st: st["_hist_max_all"] = 0.0
                st["_hist_max"] = 0.0
                st["_hist_max_all"] = 0.0
                inc[i] = 0.0; inc_all[i] = 0.0
                state[key] = st
                continue

            # compute TrueShort increments
            if "_hist_max" not in st: st["_hist_max"] = 0.0
            if "_hist_max_all" not in st: st["_hist_max_all"] = 0.0

            add = 0.0
            if c > st["_hist_max"]:
                add = c - st["_hist_max"]
                st["_hist_max"] = c
            inc[i] = add if is_wh else 0.0

            add_all = 0.0
            if c > st["_hist_max_all"]:
                add_all = c - st["_hist_max_all"]
                st["_hist_max_all"] = c
            inc_all[i] = add_all

            state[key] = st

        # flush windows at end
        for key in list(state.keys()):
            push_window(key)

        df["TrueShortQty_WH"]  = inc
        df["TrueShortQty_ALL"] = inc_all

        # ------ Rogue orderlines (>80% of that PIP’s total TrueShort across ALL routes) ------
        pip_total = df.groupby(df[pip].astype(str))["TrueShortQty_ALL"].sum()
        big_line = df["TrueShortQty_ALL"] > 0
        contrib = pd.Series(False, index=df.index)
        if not pip_total.empty:
            thresh = df[pip].astype(str).map(pip_total) * 0.8
            contrib = df["TrueShortQty_ALL"] > thresh

        rogue_lines = df.loc[contrib].copy()
        rogue_count = int(rogue_lines.shape[0])

        # Final cleaned frame used for rollups = metric rows & not rogue
        roll_mask_base = metric_sorted & (~contrib)

        # ------ Orderlist rollup (ALL routes) ------
        completed_only = args.completed_only

        def rollup_orderlist_all(
            df_ref: pd.DataFrame,
            eligible_mask: pd.Series,
            denom_map: dict[str, float],
        ) -> pd.DataFrame:
            """Build the orderlist roll-up across all order routes."""

            mask_metric = eligible_mask.reindex(df_ref.index, fill_value=False)
            if completed_only:
                mask_metric &= df_ref["_IsCompleted"].fillna(False)

            grouped = (
                df_ref.loc[mask_metric]
                      .groupby("Orderlist_Final", dropna=False)
                      .agg(
                          true_short_qty=("TrueShortQty_ALL", "sum"),
                          true_short_lines=("TrueShortQty_ALL", lambda s: (s > 0).sum()),
                      )
                      .reset_index()
            )

            denom_df = (
                pd.DataFrame(
                    [(k, v) for k, v in denom_map.items()],
                    columns=["Orderlist_Final", "dedup_req_qty"],
                )
                if denom_map
                else pd.DataFrame(columns=["Orderlist_Final", "dedup_req_qty"])
            )

            summary = denom_df.merge(grouped, on="Orderlist_Final", how="outer")
            summary = summary.fillna({
                "dedup_req_qty": 0.0,
                "true_short_qty": 0.0,
                "true_short_lines": 0.0,
            })
            summary = add_share_and_rate(summary, "true_short_qty", "dedup_req_qty")

            summary = summary.rename(columns={"Orderlist_Final": "Orderlist"})
            wh_like_set = set(wh_list)
            orderlist_cf = summary["Orderlist"].astype("string").fillna("").str.casefold()
            wh_like_mask = orderlist_cf.isin(wh_like_set)

            comp_vals = summary.loc[wh_like_mask, ["true_short_qty", "true_short_lines", "dedup_req_qty"]].sum()
            comp = pd.DataFrame([
                {
                    "Orderlist": "Company (WH-like)",
                    "true_short_qty": float(comp_vals.get("true_short_qty", 0.0)),
                    "true_short_lines": float(comp_vals.get("true_short_lines", 0.0)),
                    "dedup_req_qty": float(comp_vals.get("dedup_req_qty", 0.0)),
                }
            ])
            comp = add_share_and_rate(comp, "true_short_qty", "dedup_req_qty")

            out = pd.concat([comp, summary], ignore_index=True)
            return out.sort_values("true_short_qty", ascending=False)

        # ------ Warehouse-only rollups ------
        def rollup_wh(df_ref, by_series, title, denom_map):
            wh_m = wh_sorted.reindex(df_ref.index, fill_value=False)
            mask = roll_mask_base.reindex(df_ref.index, fill_value=False) & wh_m
            if by_series is None:
                return pd.DataFrame(columns=[title,"true_short_qty","true_short_lines","dedup_req_qty","true_short_qty_pct","shortage_rate_pct"])
            key_series = by_series.rename(title)
            g = (
                df_ref.loc[mask]
                      .groupby(key_series, dropna=False)
                      .agg(true_short_qty=("TrueShortQty_WH","sum"),
                           true_short_lines=("TrueShortQty_WH", lambda s: (s>0).sum()))
                      .reset_index()
            )
            denom_df = pd.DataFrame([(k, v) for k, v in denom_map.items()], columns=[title, "dedup_req_qty"])
            g = g.merge(denom_df, on=title, how="left").fillna({"dedup_req_qty":0.0})
            g = add_share_and_rate(g, "true_short_qty", "dedup_req_qty")
            return g.sort_values(by=["true_short_qty","true_short_lines"], ascending=False)

        # ------ Clean dataframe for rollups: drop unmatched & subs ------
        df_clean = df.loc[~unmatched_mask & (~is_sub)].copy()

        r_dep = rollup_wh(df_clean, df_clean["Department_Final"], "Department", denom_dep)
        r_sup = rollup_wh(df_clean, df_clean["SupplierName_Final"], "SupplierName", denom_sup)
        r_grp = rollup_wh(df_clean, df_clean["Group_Final"], "Group", denom_grp)
        r_ord = rollup_orderlist_all(df_clean, roll_mask_base, denom_ord_all)

        # ------ Branch_NC & Company_NC (only WH & WH-CD orderlists) ------
        nc_mask_final = roll_mask_base & nc_sorted
        branch_nc = (
            df.loc[nc_mask_final.to_numpy()]
              .groupby("_Branch_NC", dropna=False)
              .size().reset_index(name="unique_order_lines")  # lines, not unique PIPs
        )
        company_nc = pd.DataFrame([{
            "company_total_unique_order_lines": int(branch_nc["unique_order_lines"].sum())
        }])

        # ------ Top_Short_PIPs (Top 50) ------
        pip_group = (
            df_clean.loc[roll_mask_base.reindex(df_clean.index, fill_value=False).to_numpy()]
              .groupby(["PIPCode","Department_Final","Group_Final","SupplierName_Final","Product_Description","Pack_Size"], dropna=False)
              .agg(true_short_qty=("TrueShortQty_WH","sum"),
                   branches_involved=(oc["branch"], lambda s: pd.Series(s).nunique()))
              .reset_index()
        )
        total_ts_wh = pip_group["true_short_qty"].sum()
        pip_group["true_short_qty_pct"] = (pip_group["true_short_qty"] / total_ts_wh) if total_ts_wh else 0.0
        top_pips = pip_group.sort_values(by="true_short_qty", ascending=False).head(50).copy()
        top_pips = top_pips.rename(columns={"PIPCode":"PIP"})

        # ------ Top_Short_Lines (with Product & Pack) ------
        top_lines = (
            df_clean.loc[(roll_mask_base.reindex(df_clean.index, fill_value=False).to_numpy()) & (df_clean["TrueShortQty_WH"]>0),
                         [oc["branch"], oc["completed"], oc["orderno"],
                          "SupplierName_Final","Orderlist_Final", oc["pipcode"], "Product_Description","Pack_Size",
                          "Department_Final","Group_Final",
                          "_Req","_Ord","_EffDel","TrueShortQty_WH"]]
              .rename(columns={
                  oc["branch"]:"Branch",
                  oc["completed"]:"Completed Date",
                  oc["orderno"]:"Branch Order No.",
                  oc["pipcode"]:"PIPCode",
                  "_Req":"Req Qty (capped?)",
                  "_Ord":"Order Qty",
                  "_EffDel":"Deliver Qty (effective)",
                  "TrueShortQty_WH":"TrueShortQty"
              })
              .sort_values(by="TrueShortQty", ascending=False)
        )

        # ------ Mismatch (Deliver != Order), info only ------
        mis_mask = df["_Del"] != df["_Ord"]
        mis_cols = [c for c in [oc["pipcode"], oc["department"], oc["branch"], oc["req"], oc["ord"], oc["delv"], oc["completed"]] if c]
        mis_detail = df.loc[mis_mask, mis_cols].copy() if mis_cols else pd.DataFrame()

        if not mis_mask.any() or not oc["pipcode"]:
            mis_summary = pd.DataFrame(columns=[
                "PIPCode",
                "productName",
                "packSize",
                "binLocation",
                "mismatch_qty_difference",
            ])
        else:
            mismatched_rows = df.loc[mis_mask].copy()
            mismatch_diff = mismatched_rows["_Ord"] - mismatched_rows["_Del"]

            summary_df = pd.DataFrame({
                "PIPCode": mismatched_rows[oc["pipcode"]].astype("string"),
                "mismatch_qty_difference": mismatch_diff,
            })

            if prod_name:
                summary_df["productName"] = mismatched_rows[prod_name].astype("string")
            else:
                summary_df["productName"] = ""

            if prod_pack:
                summary_df["packSize"] = mismatched_rows[prod_pack].astype("string")
            else:
                summary_df["packSize"] = ""

            if prod_bin:
                summary_df["binLocation"] = mismatched_rows[prod_bin].astype("string")
            else:
                summary_df["binLocation"] = ""

            mis_summary = (
                summary_df
                .groupby(["PIPCode", "productName", "packSize", "binLocation"], dropna=False)
                .agg(mismatch_qty_difference=("mismatch_qty_difference", "sum"))
                .reset_index()
            )

            if not mis_summary.empty:
                mis_summary["abs_mismatch_qty_difference"] = mis_summary["mismatch_qty_difference"].abs()
                mis_summary = mis_summary.sort_values(
                    ["abs_mismatch_qty_difference", "mismatch_qty_difference"],
                    ascending=[False, False],
                ).drop(columns="abs_mismatch_qty_difference")

        # ------ Unmatched tabs ------
        unmatched_pips_tab = (
            unmatched_pips.value_counts()
            .rename_axis("PIPCode")
            .reset_index(name="unmatched_count")
            .sort_values(by="unmatched_count", ascending=False)
        )
        unmatched_orderlines_tab = unmatched_detail[
            [oc["branch"], oc["orderno"], oc["completed"], oc["pipcode"], oc["req"], oc["ord"], oc["delv"], "Orderlist_Final"]
        ].rename(columns={
            oc["branch"]:"Branch",
            oc["orderno"]:"Branch Order No.",
            oc["completed"]:"Completed Date",
            oc["pipcode"]:"PIPCode",
            oc["req"]:"Req Qty",
            oc["ord"]:"Order Qty",
            oc["delv"]:"Deliver Qty",
        }).sort_values(by=["Branch","Branch Order No."])

        # ------ Diagnostics ------
        diag = {
            "rows_total": int(len(df)),
            "rows_completed": int(df["_IsCompleted"].sum()),
            "rows_non_completed": int((~df["_IsCompleted"]).sum()),
            "warehouse_like_rows_matched": warehouse_like_rows_matched,
            "substituted_rows": substituted_rows,
            "unmatched_pip_rows": int(unmatched_mask.sum()),
            "rule2_capped_lines": rule2_capped_lines,
            "base_short_lines": int((df["_ShortEff"]>0).sum()),
            "base_short_qty": float(df.loc[df["_ShortEff"]>0, "_ShortEff"].sum()),
            "final_true_short_lines_WH": int((df["TrueShortQty_WH"]>0).sum()),
            "final_true_short_qty_WH": float(df["TrueShortQty_WH"].sum()),
            "final_true_short_lines_ALL": int((df["TrueShortQty_ALL"]>0).sum()),
            "final_true_short_qty_ALL": float(df["TrueShortQty_ALL"].sum()),
            "rogue_order_lines_excluded": rogue_count,
        }
        diag_df = pd.DataFrame([{"metric": k, "value": v} for k,v in diag.items()])

        # derive wcDDMMYY suffix from earliest Completed Date (metric-eligible)
        if oc["completed"]:
            earliest = df.loc[metric_mask_base.to_numpy(), oc["completed"]].dropna()
            if not earliest.empty:
                dt = pd.to_datetime(earliest, errors="coerce", dayfirst=True).min()
                if pd.notna(dt):
                    suffix = "_wc" + dt.strftime("%d%m%y")

        final_out = out_path.with_name(out_path.stem + suffix + out_path.suffix)
        log(f"Writing Excel: {final_out}")

        with pd.ExcelWriter(final_out, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
            sheets = [
                ("Dept_Shortage", r_dep),
                ("Supplier_Shortage", r_sup),
                ("Group_Shortage", r_grp),
                ("Orderlist_Shortage", r_ord),
                ("Top_Short_PIPs", top_pips),
                ("Top_Short_Lines", top_lines),
                ("Mismatch_Detail", mis_detail),
                ("Mismatch_Summary", mis_summary),
                ("Branch_NC", branch_nc),
                ("Company_NC", company_nc),
                ("Unmatched_PIPs", unmatched_pips_tab),
                ("Unmatched_Orderlines", unmatched_orderlines_tab),
                ("Outlier_Orders_gt80pct", rogue_lines[
                    [oc["branch"], oc["orderno"], oc["completed"], oc["pipcode"], "Product_Description","Pack_Size",
                     "Orderlist_Final","Department_Final","Group_Final","SupplierName_Final",
                     "_Req","_Ord","_EffDel","TrueShortQty_ALL"]
                ].rename(columns={
                    oc["branch"]:"Branch", oc["orderno"]:"Branch Order No.", oc["completed"]:"Completed Date",
                    oc["pipcode"]:"PIPCode", "_Req":"Req Qty", "_Ord":"Order Qty", "_EffDel":"Deliver Qty (effective)",
                })),
                ("Diagnostics", diag_df),
                ("Orders_Enriched", df)  # full trace (includes flags, useful for audit)
            ]
            for name, data in sheets:
                data.to_excel(xw, index=False, sheet_name=name)
                autosize_sheet(xw, name, data)
                apply_formats(xw, name, data)

        log("DONE.")
        return 0

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        with open(errlog, "w", encoding="utf-8") as fh:
            fh.write(tb)
        print(tb, file=sys.stderr)
        print(f"[!] Error logged to: {errlog}", file=sys.stderr)
        return 2

if __name__ == "__main__":
    sys.exit(main())
