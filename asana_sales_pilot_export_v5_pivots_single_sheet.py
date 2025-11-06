
# -*- coding: utf-8 -*-
# Asana Sales Pilot Export - v5 (defensive pivots + Excel Table on 'data')
# - Hoja 'data' ahora se escribe como **Tabla de Excel** (con filtros y formato).
# - Hoja 'Tablas Dinamicas' crea pivots si existe add_pivot_table; si no, deja instrucciones.
# - Safe-write: escribe a .tmp y reemplaza, o guarda copia con timestamp si el archivo está abierto.

import os, sys, re, json, argparse, time
from typing import Any, Dict, List, Optional
from datetime import datetime

import pandas as pd
from dateutil import parser as dateparse

import asana
from asana.rest import ApiException

RUN_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(RUN_DIR, "out")
DEFAULT_OUT_XLSX = os.path.join(OUT_DIR, "sales_pilot.xlsx")
DATE_FMT = "%m/%d/%Y"

COLUMN_STATUS_MAP = {
    "PROBLEMS IN POR...": "Problems",
    "PROBLEMS IN PRO...": "Problems",
    "RECALL": "Recall",
    "STAND BY": "Stand By",
    "UNSCHEDULED": "Unscheduled",
    "SCHEDULED": "Scheduled",
    "REJECT DP. PROPOSAL": "Rejected Proposal",
    "WITHOUT PROPOS...": "Without Proposal",
    "QUALITY CONTROL": "Quality Control",
    "PENDING INVOICE": "Pending Invoice",
    "PENDING PAYMENTS": "Pending Payments",
    "DONE": "Done",
    "CANCELED": "Canceled",
}
IGNORED_SECTION_TOKENS = {"TRASH", "ARCHIVE"}

FIELD_ALIASES = {
    "client": {"CLIENTE", "CLIENT", "CLIENT NAME", "CLIENTE NOMBRE", "CUSTOMER"},
    "zone": {"ZONA", "ZONE"},
    "approved_value": {"VALOR APROBADO", "VALOR TOTAL", "APPROVED AMOUNT", "AMOUNT APPROVED", "APPROVED VALUE"},
    "invoice_date": {"FECHA DE INVOICE", "FECHA FACTURA", "INVOICE ENVIADO", "INVOICE DATE"},
    "paid_flag": {"PAGADO", "PAID"},
    "paid_date": {"FECHA DE PAGO", "PAID DATE"},
    "paid_amount": {"MONTO PAGADO", "PAGADO (MONTO)", "PAID AMOUNT"},
    "priority": {"PRIORITY", "PRIORIDAD"},
    "status": {"STATUS", "ESTADO", "ESTATUS"},
    "canceled_flag": {"CANCELADA", "CANCELED"},
    "canceled_date": {"FECHA DE CANCELACION", "FECHA DE CANCELACIÓN", "CANCEL DATE"},
    "type": {"TIPO", "TYPE"},
    "wo_number": {"WO #", "WO", "WORK ORDER"},
    "materials_cost": {"GASTO MATERIALES","MATERIALES","MATERIAL COST","COSTO MATERIALES"},
    "labor_cost": {"GASTO LABOR","GATOS LABOR","GASTO MANO DE OBRA","LABOR COST","MANO DE OBRA","GASTO LABOUR","GASTOS LABOR"},
}
OPT_FIELDS = [
    "name","completed","permalink_url","created_at","modified_at","notes",
    "memberships.project.name","memberships.project.gid","memberships.section.name",
    "custom_fields.name","custom_fields.display_value","custom_fields.enum_value.name",
    "custom_fields.number_value","custom_fields.text_value","custom_fields.type",
]
RE_WO_ANY = re.compile(r"\bWO\s+([A-Za-z0-9]+(?:\s*[-/]\s*\d+)*)", re.IGNORECASE)


def normalize_upper(s: str) -> str:
    import unicodedata
    if not s: return ""
    n = unicodedata.normalize("NFD", s)
    n = "".join(ch for ch in n if unicodedata.category(ch) != "Mn")
    return n.upper().strip()

def _alias_hit(field_name_norm: str, alias_set: set) -> bool:
    if field_name_norm in alias_set:
        return True
    for a in alias_set:
        if a in field_name_norm:
            return True
    return False

def parse_client_from_text(name: str, notes: str) -> str:
    blob = f"{name}\n{notes}"
    m = re.search(r"(?is)(?:^|\r?\n)\s*(?:client|cliente)\s*[:\-]\s*([^\r\n]+)", blob)
    if not m:
        m = re.search(r"(?is)(?:client|cliente)\s*[:\-]\s*([^\r\n]+)", blob)
    if m:
        val = m.group(1).strip()
        val = re.sub(r"\s{2,}", " ", val)
        # corta si viene priority/zone/status pegado
        val = re.split(r"\s+(?:PRIORITY|MEDIUM|HIGH|LOW|STATUS|ZONE)\b", val, flags=re.I)[0]
        return val.strip(" -:;|")
    return ""


def wo_from_name(text: str) -> str:
    if not text:
        return ""
    m = RE_WO_ANY.search(text)
    if m:
        return f"WO {m.group(1).strip()}"
    # fallback: corta antes de priority/zone/status si viniera todo junto
    m2 = re.search(r"\bWO\b.*?(?=\s+(?:PRIORITY|MEDIUM|HIGH|LOW|STATUS|ZONE)\b|$)", text, flags=re.I)
    if m2:
        m3 = re.search(r"\bWO\s+([A-Za-z0-9]+(?:\s*[-/]\s*\d+)*)", m2.group(0), flags=re.I)
        if m3:
            return f"WO {m3.group(1).strip()}"
        return m2.group(0).strip()
    return ""


def zone_from_project(project_name: str) -> str:
    if not project_name: return ""
    return project_name.split()[-1].strip(",")

def _try_parse_money(raw: Optional[str]) -> Optional[float]:
    if not raw: return None
    s = str(raw).strip().upper()
    s = re.sub(r"[^\\d,.\\-]", "", s)
    if not s: return None
    if "," in s and "." in s:
        try:
            return float(s.replace(",", ""))
        except Exception:
            pass
    if "," in s and "." not in s:
        try:
            return float(s.replace(",", "."))
        except Exception:
            pass
    try:
        return float(s)
    except Exception:
        return None

def _fmt_mmddyyyy(val: str) -> str:
    if not val:
        return ""
    try:
        dt = dateparse.parse(str(val), fuzzy=True)
        return dt.strftime("%m/%d/%Y")
    except Exception:
        s = str(val)
        if "T" in s:
            s = s.split("T", 1)[0]
        try:
            return datetime.strptime(s, "%Y-%m-%d").strftime("%m/%d/%Y")
        except Exception:
            return s

def load_fees_map(path: str) -> Dict[str, Dict[str, float]]:
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    norm = {}
    for client, types in data.items():
        c = (client or "").upper()
        norm[c] = {}
        for t, pct in (types or {}).items():
            norm[c][(t or "").upper()] = float(pct)
    return norm

def get_fee_pct(fees_map: Dict[str,Dict[str,float]], client: str, ttype: str) -> float:
    c = (client or "").upper()
    tt = (ttype or "").upper()
    if c in fees_map:
        if tt in fees_map[c]:
            return float(fees_map[c][tt])
        if "ANY" in fees_map[c]:
            return float(fees_map[c]["ANY"])
    return 0.0

def build_clients(token: str):
    if not token:
        raise RuntimeError("Falta token. Exporta ASANA_ACCESS_TOKEN o usa --token.")
    cfg = asana.Configuration(); cfg.access_token = token
    api_client = asana.ApiClient(cfg)
    return {"tasks": asana.TasksApi(api_client), "projects": asana.ProjectsApi(api_client)}

def fetch_project_tasks(tasks_api, project_gid: str):
    return list(tasks_api.get_tasks_for_project(project_gid, {"opt_fields": ",".join(OPT_FIELDS)}, item_limit=100000))

def extract_fields(task: Dict[str,Any]) -> Dict[str,Any]:
    out = {
        "client": "", "zone": "", "approved_value": None,
        "invoice_date": "", "paid_flag": False, "paid_date": "", "paid_amount": None,
        "priority": "", "status": "", "canceled_flag": False, "canceled_date": "",
        "type": "", "wo_number": "", "materials_cost": 0.0, "labor_cost": 0.0
    }
    for cf in task.get("custom_fields") or []:
        name = cf.get("name") or ""
        n = normalize_upper(name)
        disp = cf.get("display_value")
        enumn = (cf.get("enum_value") or {}).get("name")
        num  = cf.get("number_value")
        text = cf.get("text_value")

        if _alias_hit(n, FIELD_ALIASES["client"]):
            out["client"] = disp or text or ""
        elif _alias_hit(n, FIELD_ALIASES["zone"]):
            out["zone"] = disp or text or enumn or ""
        elif _alias_hit(n, FIELD_ALIASES["approved_value"]):
            out["approved_value"] = num if num is not None else _try_parse_money(disp or text)
        elif _alias_hit(n, FIELD_ALIASES["invoice_date"]):
            out["invoice_date"] = (disp or text or "").strip()
        elif _alias_hit(n, FIELD_ALIASES["paid_flag"]):
            val = (disp or text or enumn or "").strip().lower()
            out["paid_flag"] = val in {"1","true","yes","sí","si"} or val == "paid"
        elif _alias_hit(n, FIELD_ALIASES["paid_date"]):
            out["paid_date"] = (disp or text or "").strip()
        elif _alias_hit(n, FIELD_ALIASES["paid_amount"]):
            out["paid_amount"] = num if num is not None else _try_parse_money(disp or text)
        elif _alias_hit(n, FIELD_ALIASES["priority"]):
            out["priority"] = enumn or disp or text or ""
        elif _alias_hit(n, FIELD_ALIASES["status"]):
            out["status"] = enumn or disp or text or ""
        elif _alias_hit(n, FIELD_ALIASES["canceled_flag"]):
            val = (disp or text or enumn or "").strip().lower()
            out["canceled_flag"] = val in {"1","true","yes","sí","si"} or val == "canceled"
        elif _alias_hit(n, FIELD_ALIASES["canceled_date"]):
            out["canceled_date"] = (disp or text or "").strip()
        elif _alias_hit(n, FIELD_ALIASES["type"]):
            out["type"] = (enumn or disp or text or "").strip()
        elif _alias_hit(n, FIELD_ALIASES["wo_number"]):
            out["wo_number"] = (disp or text or "").strip()
        elif _alias_hit(n, FIELD_ALIASES["materials_cost"]):
            out["materials_cost"] = (num if num is not None else _try_parse_money(disp or text)) or 0.0
        elif _alias_hit(n, FIELD_ALIASES["labor_cost"]):
            out["labor_cost"] = (num if num is not None else _try_parse_money(disp or text)) or 0.0

    if not out["wo_number"]:
        out["wo_number"] = wo_from_name(task.get("name") or "")
    return out

def build_dataframe(token: str, projects: List[Dict[str,str]], done_cutoff: str, fees_map: Dict[str,Dict[str,float]]):
    clients = build_clients(token)
    rows: List[Dict[str,Any]] = []

    cutoff_date = None
    if done_cutoff:
        try:
            from dateutil import parser as _dp
            cutoff_date = _dp.parse(done_cutoff).date()
        except Exception:
            cutoff_date = None

    for p in projects:
        pgid = str(p["gid"]); pname = p.get("name","")
        project_zone = zone_from_project(pname)
        ttype = ("WO" if "(WO)" in pname.upper() else ("PO" if "(PO)" in pname.upper() else ""))
        tasks = fetch_project_tasks(clients["tasks"], pgid)

        for t in tasks:
            section = ""
            for m in t.get("memberships") or []:
                pr = (m or {}).get("project") or {}
                if str(pr.get("gid")) == pgid:
                    sec = (m or {}).get("section") or {}
                    section = (sec.get("name") or "").strip()
                    break
            if any(tok in normalize_upper(section) for tok in IGNORED_SECTION_TOKENS):
                continue
            if cutoff_date:
                sec_norm = normalize_upper(section)
                if ("DONE" in sec_norm or "CANCEL" in sec_norm):
                    _ts = t.get("modified_at") or t.get("created_at") or ""
                    try:
                        from dateutil import parser as _dp
                        _d = _dp.parse(_ts).date()
                    except Exception:
                        _d = None
                    if _d and _d < cutoff_date:
                        continue

            cf = extract_fields(t)
            if not cf["zone"]:
                cf["zone"] = project_zone
            if not cf.get("client"):
                parsed_client = parse_client_from_text(t.get("name") or "", t.get("notes") or "")
                if parsed_client:
                    cf["client"] = parsed_client

            approved = float(cf["approved_value"] or 0.0)
            fee_pct  = get_fee_pct(fees_map, cf["client"], ttype)
            approved_real = approved * (1.0 - fee_pct)
            materials = float(cf.get("materials_cost") or 0.0)
            labor = float(cf.get("labor_cost") or 0.0)
            gastos_total = materials + labor
            profit = approved_real - gastos_total
            profit_pct = (profit / approved_real) if approved_real else 0.0

            invoice_date = cf["invoice_date"]
            invoice_month = ""
            if invoice_date:
                try:
                    dt = dateparse.parse(invoice_date, fuzzy=True)
                    invoice_month = dt.strftime("%Y-%m")
                except Exception:
                    invoice_month = ""

            rows.append({
                "Project": pname,
                "Zone": cf["zone"],
                "Type": cf["type"] or ttype,
                "Task GID": t.get("gid"),
                "Task URL": t.get("permalink_url"),
                "# de orden": (cf["wo_number"] or "").strip(),
                "Task Name": t.get("name") or "",
                "Column": section,
                "Logical Status": COLUMN_STATUS_MAP.get(normalize_upper(section), section),
                "Client": cf["client"],
                "Priority": cf["priority"],
                "Status (CF)": cf["status"],
                "Approved Value": approved,
                "Valor Aprobado (Real)": approved_real,
                "Gasto Materiales": materials,
                "Gasto Labor": labor,
                "Gastos Total": gastos_total,
                "Utilidad ($)": profit,
                "Utilidad (%)": profit_pct,
                "Invoice Date": _fmt_mmddyyyy(invoice_date),
                "Invoice Month": invoice_month,
                "Paid?": bool(cf["paid_flag"] or bool(cf["paid_date"])),
                "Paid Date": _fmt_mmddyyyy(cf["paid_date"]),
                "Paid Amount": cf["paid_amount"],
                "Created At (Asana)": t.get("created_at") or "",
                "Modified At": t.get("modified_at") or "",
            })

    df = pd.DataFrame(rows)
    desired = ["Project","Zone","Type","# de orden","Priority","Client","Column","Status (CF)",
               "Approved Value","Valor Aprobado (Real)","Gasto Materiales","Gasto Labor","Gastos Total",
               "Utilidad ($)","Utilidad (%)","Invoice Date","Invoice Month","Paid?","Paid Date","Paid Amount"]
    existing = list(df.columns)
    df = df[[c for c in desired if c in existing] + [c for c in existing if c not in desired]]
    return df

def write_excel_with_pivots(df: pd.DataFrame, out_path: str):
    import xlsxwriter
    print(f"XlsxWriter version: {xlsxwriter.__version__} -> {xlsxwriter.__file__}")
    try:
        from xlsxwriter.worksheet import Worksheet as _WS
        print("Worksheet has add_pivot_table?:", hasattr(_WS, "add_pivot_table"))
    except Exception as _e:
        print("Diag Worksheet import error:", _e)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    base, ext = os.path.splitext(out_path)
    temp_path = f"{base}.tmp{ext}"
    alt_path  = f"{base}_{int(time.time())}{ext}"

    with pd.ExcelWriter(temp_path, engine="xlsxwriter") as writer:
        # --- hoja data como Tabla de Excel ---
        df.to_excel(writer, sheet_name="data", index=False)
        wb  = writer.book
        ws  = writer.sheets["data"]

        # Formatos
        usd = wb.add_format({'num_format': '$#,##0.00'})
        pct = wb.add_format({'num_format': '0.00%'})
        for col_name in ["Approved Value","Valor Aprobado (Real)","Gasto Materiales","Gasto Labor","Gastos Total","Paid Amount","Utilidad ($)"]:
            if col_name in df.columns:
                col_idx = df.columns.get_loc(col_name)
                ws.set_column(col_idx, col_idx, 14, usd)
        if "Utilidad (%)" in df.columns:
            col_idx = df.columns.get_loc("Utilidad (%)")
            ws.set_column(col_idx, col_idx, 12, pct)

        # Convertir rango a Tabla de Excel (con filtros)
        from string import ascii_uppercase
        def col_letter(n: int) -> str:
            s=""
            while n>0:
                n, r = divmod(n-1, 26)
                s = ascii_uppercase[r] + s
            return s
        max_row, max_col = len(df)+1, len(df.columns)
        end_col = col_letter(max_col)
        data_range = f"data!$A$1:${end_col}${max_row}"
        # Cabezeras para add_table
        table_columns = [{"header": h} for h in df.columns]
        ws.add_table(0, 0, max_row-1, max_col-1, {
            "name": "data_table",
            "columns": table_columns,
            "style": "Table Style Medium 9"
        })
        ws.freeze_panes(1, 0)

        # --- hoja de pivots (si está disponible) ---
        piv = wb.add_worksheet("Tablas Dinamicas")

        if hasattr(piv, "add_pivot_table"):
            try:
                piv.add_pivot_table({
                    'data':       data_range,
                    'name':       'PT_InGeneral',
                    'row_fields': ['Type'],
                    'filters':    ['Invoice Month', 'Status (CF)'],
                    'values': [
                        {'name': 'Valor Aprobado (Real)', 'function': 'sum', 'num_format': '$#,##0.00', 'caption': 'Ventas'},
                        {'name': 'Utilidad ($)', 'function': 'sum', 'num_format': '$#,##0.00', 'caption': 'Utilidad'},
                        {'name': 'Gastos Total', 'function': 'sum', 'num_format': '$#,##0.00', 'caption': 'Gastos'},
                        {'name': '# de orden', 'function': 'count', 'caption': '# de Orden'}
                    ],
                    'row': 0, 'col': 0
                })
                piv.add_pivot_table({
                    'data':       data_range,
                    'name':       'PT_WO_Zona',
                    'row_fields': ['Zone'],
                    'filters':    ['Invoice Month', 'Type', 'Status (CF)'],
                    'values': [
                        {'name': 'Valor Aprobado (Real)', 'function': 'sum', 'num_format': '$#,##0.00', 'caption': 'Ventas'},
                        {'name': 'Utilidad ($)', 'function': 'sum', 'num_format': '$#,##0.00', 'caption': 'Utilidad'},
                        {'name': 'Gastos Total', 'function': 'sum', 'num_format': '$#,##0.00', 'caption': 'Gastos'},
                        {'name': '# de orden', 'function': 'count', 'caption': '# de Orden'}
                    ],
                    'row': 19, 'col': 0
                })
                piv.add_pivot_table({
                    'data':       data_range,
                    'name':       'PT_PO_Zona',
                    'row_fields': ['Zone'],
                    'filters':    ['Invoice Month', 'Type', 'Status (CF)'],
                    'values': [
                        {'name': 'Valor Aprobado (Real)', 'function': 'sum', 'num_format': '$#,##0.00', 'caption': 'Ventas'},
                        {'name': 'Utilidad ($)', 'function': 'sum', 'num_format': '$#,##0.00', 'caption': 'Utilidad'},
                        {'name': 'Gastos Total', 'function': 'sum', 'num_format': '$#,##0.00', 'caption': 'Gastos'},
                        {'name': '# de orden', 'function': 'count', 'caption': '# de Orden'}
                    ],
                    'row': 39, 'col': 0
                })
            except AttributeError as e:
                piv.write(0, 0, "Tu XlsxWriter reportó versión >=3.1.0 pero no expone add_pivot_table en Worksheet.")
                piv.write(1, 0, "Soluciones sugeridas:")
                piv.write(2, 0, "1) Fuerza reinstalación limpia:")
                piv.write(3, 0, r'   py -3.14 -m pip install --force-reinstall --no-cache-dir "XlsxWriter>=3.2.0"')
                piv.write(4, 0, "2) Asegúrate de ejecutar ESTE script con el Python 3.14 de la ruta larga (no el del PATH).")
                piv.write(5, 0, "3) Si persiste, reinicia PowerShell / PC (cacheos).")
                piv.write(6, 0, f"Detalle AttributeError: {str(e)}")
        else:
            piv.write(0, 0, "Este entorno no expone Worksheet.add_pivot_table (XlsxWriter antiguo o env conflict).")
            piv.write(1, 0, "Instala/actualiza XlsxWriter 3.2.x y ejecuta con el Python correcto.")
            piv.write(2, 0, r'py -3.14 -m pip install --force-reinstall --no-cache-dir "XlsxWriter>=3.2.0"')
            piv.write(4, 0, "Mientras tanto, ya dejé la hoja 'data' como **Tabla de Excel** llamada data_table.")
            piv.write(5, 0, "Puedes crear tablas dinámicas manuales desde esa tabla y quedarán conectadas.")

    # Reemplazo seguro
    try:
        if os.path.exists(out_path):
            os.replace(temp_path, out_path)
        else:
            os.rename(temp_path, out_path)
        return out_path
    except PermissionError:
        os.replace(temp_path, alt_path)
        return alt_path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--projects-json", required=True)
    ap.add_argument("--token", default=os.getenv("ASANA_ACCESS_TOKEN",""))
    ap.add_argument("--fees-json", default=os.path.join(RUN_DIR,"fees.json"))
    ap.add_argument("--out", default=DEFAULT_OUT_XLSX)
    ap.add_argument("--done-cutoff", default=os.getenv("ASANA_DONE_CUTOFF",""))
    args = ap.parse_args()

    if not os.path.exists(args.projects_json):
        print("❌ No existe projects.json", file=sys.stderr); sys.exit(2)
    projects = json.load(open(args.projects_json,"r",encoding="utf-8"))
    fees_map = {}
    if os.path.exists(args.fees_json):
        with open(args.fees_json,"r",encoding="utf-8") as f:
            fees_map = json.load(f)

    try:
        df = build_dataframe(args.token, projects, args.done_cutoff, fees_map)
        path = write_excel_with_pivots(df, args.out)
        print(f"✅ Archivo listo: {os.path.abspath(path)}")
        if path != args.out:
            print("⚠️ No se pudo reemplazar el archivo destino (¿abierto en Excel?). Guardé una copia alternativa.")
            print(f"   Copia: {os.path.abspath(path)}")
        print("ℹ️ Ejecuta con el MISMO Python donde validaste XlsxWriter (tu 3.14 de ruta larga).")
    except ApiException as e:
        print(f"❌ API error: {getattr(e,'status','?')} {getattr(e,'reason','')}", file=sys.stderr); sys.exit(3)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr); sys.exit(4)

if __name__ == "__main__":
    main()
