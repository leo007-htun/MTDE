"""
Generates AMOS MTDE API Reference PDF.
Run: python scripts/generate_api_doc.py
Output: AMOS_MTDE_API_Reference.pdf
"""
from fpdf import FPDF, XPos, YPos
from datetime import datetime, timedelta, timezone
import json, math

BASE_URL      = "https://mtde-production.up.railway.app"
DASHBOARD_URL = "https://brave-nature-production-8e18.up.railway.app"
OUT_FILE      = "AMOS_MTDE_API_Reference.pdf"

NOW    = "2026-05-06T03:10:00+00:00"
BASE_T = datetime(2026, 5, 6, 3, 0, 0, tzinfo=timezone.utc)

# ── Sample payloads ────────────────────────────────────────────────────────────

SENSOR_PAYLOAD = {
    "farm_id": "farm_00",
    "timestamp": NOW,
    "panels": [
        {"panel_id": "farm_00_panel_00", "timestamp": NOW,
         "power_kw": 42.5, "irradiance_wm2": 750.0,
         "inverter_temp_c": 42.5, "ambient_temp_c": 20.0},
        {"panel_id": "farm_00_panel_01", "timestamp": NOW,
         "power_kw": 43.0, "irradiance_wm2": 752.0,
         "inverter_temp_c": 43.1, "ambient_temp_c": 20.0},
    ]
}

IDB_PAYLOAD = {
    "timestamp": NOW,
    "battery_soc_kwh": 620.5,
    "battery_soc_max_kwh": 1000.0,
    "battery_temp_c": 31.2,
    "battery_power_kw": 45.0,
    "solar_power_kw": 380.0,
    "grid_exchange_kw": -25.0,
    "compressor_vibration_g": 0.12,
    "compressor_load_pct": 48.5,
}

MARKET_PAYLOAD = {
    "timestamp": NOW,
    "price_per_kwh": 0.2843,
    "carbon_intensity_gco2_kwh": 214.0,
}

forecasts, timestamps = [], []
for h in range(48):
    t  = BASE_T + timedelta(hours=h)
    hr = t.hour
    sf = max(0.0, math.sin(math.pi * (hr - 6) / 12)) if 6 <= hr <= 18 else 0.0
    forecasts.append(round(sf * 420.0, 2))
    timestamps.append(t.strftime("%Y-%m-%dT%H:%M:%S+00:00"))

TTA_PAYLOAD = {
    "data_id": "solar1_meter_1",
    "timestamp": NOW,
    "adapted_predictions_denorm": forecasts[:4] + ["... 44 more kW values ..."],
    "prediction_timestamps":      timestamps[:4] + ["... 44 more timestamps ..."],
    "adaptation_gap": 0.08,
    "original_predictions_denorm": [round(v * 0.95, 2) for v in forecasts[:4]] + ["..."],
}

ENDPOINTS = [
    {
        "method": "GET",
        "path": "/health",
        "title": "1. Health Check",
        "description": "Verify the API is running and connected to RabbitMQ. Returns HTTP 503 if the broker is disconnected.",
        "request_body": None,
        "response": {"status": "ok", "rabbitmq": True},
        "fields": [],
    },
    {
        "method": "POST",
        "path": "/ingest/sensor",
        "title": "2. Solar Panel Sensor Readings",
        "description": (
            "Submit raw sensor readings from a solar farm. Each request covers one farm snapshot "
            "containing one reading per panel. Send one request per farm every 30 seconds. "
            "Forwarded to the IoT Asset Layer which computes a composite health index per panel "
            "and passes the result to the Regional MPC optimizer."
        ),
        "request_body": SENSOR_PAYLOAD,
        "response": {"status": "accepted", "farm_id": "farm_00", "panels": 2},
        "fields": [
            ("farm_id", "string", "Required", "Unique farm identifier e.g. farm_00"),
            ("timestamp", "ISO 8601", "Required", "UTC snapshot timestamp"),
            ("panels[].panel_id", "string", "Required", "Unique panel identifier"),
            ("panels[].power_kw", "float", "Required", "Measured DC power output (kW)"),
            ("panels[].irradiance_wm2", "float", "Required", "Plane-of-array irradiance (W/m2)"),
            ("panels[].inverter_temp_c", "float", "Required", "Inverter temperature (deg C)"),
            ("panels[].ambient_temp_c", "float", "Optional", "Ambient temperature, default 25.0 deg C"),
        ],
    },
    {
        "method": "POST",
        "path": "/ingest/telemetries",
        "title": "3. IDB Hardware Telemetry",
        "description": (
            "Submit real-time telemetry from IDB Protect GO hardware. Covers battery state of "
            "charge, temperature, power flow (charge/discharge), solar output, grid exchange, "
            "and compressor health metrics. Send every 30-60 seconds. Forwarded to the "
            "AI Agent Strategic Layer which uses it to adjust MPC constraints."
        ),
        "request_body": IDB_PAYLOAD,
        "response": {"status": "accepted", "battery_soc_kwh": 620.5},
        "fields": [
            ("timestamp", "ISO 8601", "Required", "UTC timestamp"),
            ("battery_soc_kwh", "float", "Required", "Battery state of charge (kWh)"),
            ("battery_soc_max_kwh", "float", "Optional", "Battery capacity, default 1000.0 kWh"),
            ("battery_temp_c", "float", "Required", "Battery pack temperature (deg C)"),
            ("battery_power_kw", "float", "Required", "Battery power: positive=charging, negative=discharging"),
            ("solar_power_kw", "float", "Optional", "Solar generation output (kW)"),
            ("grid_exchange_kw", "float", "Optional", "Grid exchange: positive=import, negative=export"),
            ("compressor_vibration_g", "float", "Optional", "Compressor vibration (g); normal < 0.3"),
            ("compressor_load_pct", "float", "Optional", "Compressor load percentage (%)"),
        ],
    },
    {
        "method": "POST",
        "path": "/ingest/market",
        "title": "4. Electricity Market Signal",
        "description": (
            "Submit the current electricity spot price and grid carbon intensity. Used by the "
            "Central Optimization Layer to maximize revenue in the fleet LP objective function "
            "(price x Generation), and by the AI Agent to adjust the economic weighting in the "
            "strategy profile. Send every 5-15 minutes or whenever the price changes significantly."
        ),
        "request_body": MARKET_PAYLOAD,
        "response": {"status": "accepted", "price_per_kwh": 0.2843, "carbon_intensity_gco2_kwh": 214.0},
        "fields": [
            ("timestamp", "ISO 8601", "Required", "UTC timestamp"),
            ("price_per_kwh", "float", "Required", "Electricity spot price (GBP/kWh)"),
            ("carbon_intensity_gco2_kwh", "float", "Required", "Grid carbon intensity (gCO2/kWh)"),
        ],
    },
    {
        "method": "POST",
        "path": "/ingest/tta",
        "title": "5. SeoulTech TTA 48-Hour Solar Forecast",
        "description": (
            "Submit a Transfer Learning with Task Adaptation (TTA) 48-step hourly solar power "
            "forecast. Consumed by two components: (1) Regional Edge Layer MPC uses the forecast "
            "as the solar generation input for the next 24 hours; (2) Consensus Layer maintains "
            "a 5-hour rolling buffer of adaptation_gap values to compute fleet health stress. "
            "A higher adaptation_gap indicates the TTA model is diverging from the base model, "
            "signalling system stress. Send once per hour."
        ),
        "request_body": TTA_PAYLOAD,
        "response": {"status": "accepted", "data_id": "solar1_meter_1", "steps": 48},
        "fields": [
            ("data_id", "string", "Required", "Meter / site identifier e.g. solar1_meter_1"),
            ("timestamp", "ISO 8601", "Required", "Forecast generation time (UTC)"),
            ("adapted_predictions_denorm", "float[48]", "Required", "48 hourly power forecast values (kW)"),
            ("prediction_timestamps", "ISO8601[48]", "Required", "One timestamp per forecast step"),
            ("adaptation_gap", "float 0-1", "Optional", "TTA adaptation magnitude; 0=healthy, default 0.0"),
            ("original_predictions_denorm", "float[48]", "Optional", "Base model forecast before TTA adaptation"),
        ],
    },
]


# ── PDF class ──────────────────────────────────────────────────────────────────

class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.set_margins(20, 22, 20)
        self.set_auto_page_break(auto=True, margin=22)

    def header(self):
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(130, 130, 130)
        self.cell(0, 6, "AMOS MTDE  |  Partner Integration API Reference",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(210, 210, 210)
        self.line(20, self.get_y(), 190, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-14)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 6,
                  f"Page {self.page_no()}  |  Generated {datetime.now().strftime('%Y-%m-%d')}  |  Liverpool John Moores University",
                  align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # ── Typography helpers ─────────────────────────────────────────────────────

    def h1(self, txt):
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(20, 60, 140)
        self.multi_cell(0, 9, txt, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)

    def h2(self, txt):
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 7, txt, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

    def h3(self, txt):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(70, 70, 70)
        self.multi_cell(0, 6, txt, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def body(self, txt):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(55, 55, 55)
        self.multi_cell(0, 6, txt, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(1)

    def bullet(self, txt):
        self.set_font("Helvetica", "", 9.5)
        self.set_text_color(55, 55, 55)
        self.multi_cell(0, 5.5, "  - " + txt, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def code_block(self, txt):
        self.set_fill_color(245, 245, 248)
        self.set_draw_color(210, 210, 210)
        self.set_font("Courier", "", 7.5)
        self.set_text_color(25, 25, 25)
        self.multi_cell(0, 4.5, txt, fill=True, border=1,
                        new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(3)

    def method_badge(self, method, path):
        colors = {"GET": (22, 120, 22), "POST": (25, 90, 180)}
        r, g, b = colors.get(method, (80, 80, 80))
        self.set_fill_color(r, g, b)
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 8)
        self.cell(16, 7, f" {method}", fill=True,
                  new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.set_fill_color(240, 242, 248)
        self.set_text_color(20, 20, 20)
        self.set_font("Courier", "", 10)
        self.cell(0, 7, f"  {path}", fill=True,
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)

    def response_badge(self, code, color=(22, 120, 22)):
        self.set_fill_color(*color)
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 8)
        self.cell(28, 6, f"  {code}", fill=True,
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)

    def field_table(self, fields):
        col = [54, 26, 22, 68]
        # Header
        self.set_fill_color(230, 235, 250)
        self.set_text_color(25, 25, 25)
        self.set_font("Helvetica", "B", 8)
        for txt, w in zip(["Field", "Type", "Req?", "Description"], col):
            self.cell(w, 7, txt, border=1, fill=True,
                      new_x=XPos.RIGHT, new_y=YPos.TOP)
        self.ln()
        # Rows
        for i, (field, ftype, req, desc) in enumerate(fields):
            bg = (250, 251, 255) if i % 2 == 0 else (255, 255, 255)
            self.set_fill_color(*bg)
            self.set_font("Courier", "", 7.5)
            self.set_text_color(25, 25, 25)
            self.cell(col[0], 6, field, border=1, fill=True,
                      new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.set_font("Helvetica", "", 8)
            self.cell(col[1], 6, ftype, border=1, fill=True,
                      new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.set_text_color(22, 120, 22 if req == "Required" else 140)
            self.cell(col[2], 6, req, border=1, fill=True,
                      new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.set_text_color(50, 50, 50)
            self.cell(col[3], 6, desc, border=1, fill=True,
                      new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(25, 25, 25)
        self.ln(3)


# ── Build ──────────────────────────────────────────────────────────────────────

def build_pdf():
    pdf = PDF()

    # ── Cover ──────────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.ln(18)
    pdf.set_font("Helvetica", "B", 30)
    pdf.set_text_color(20, 60, 140)
    pdf.cell(0, 14, "AMOS MTDE", align="C",
             new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", "B", 15)
    pdf.set_text_color(55, 55, 55)
    pdf.cell(0, 9, "Partner Integration API Reference", align="C",
             new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(3)
    pdf.set_draw_color(20, 60, 140)
    pdf.set_line_width(0.7)
    pdf.line(40, pdf.get_y(), 170, pdf.get_y())
    pdf.set_line_width(0.2)
    pdf.ln(6)
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(90, 90, 90)
    pdf.cell(0, 7, "Multi-Tier Decision Engine for Solar Microgrid Optimisation",
             align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(14)

    # URL box
    pdf.set_fill_color(244, 247, 255)
    pdf.set_draw_color(180, 200, 240)
    box_y = pdf.get_y()
    pdf.rect(28, box_y, 154, 40, style="FD")
    pdf.ln(6)
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(20, 60, 140)
    pdf.cell(0, 6, "Service Endpoints", align="C",
             new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(2)
    pdf.set_font("Courier", "", 8.5)
    pdf.set_text_color(30, 30, 30)
    for label, url in [
        ("Ingest API :", BASE_URL),
        ("Dashboard  :", DASHBOARD_URL),
        ("API Docs   :", BASE_URL + "/docs"),
    ]:
        pdf.cell(0, 5.5, f"  {label}  {url}", align="C",
                 new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(12)
    pdf.set_font("Helvetica", "", 8.5)
    pdf.set_text_color(130, 130, 130)
    pdf.cell(0, 6,
             f"Generated {datetime.now().strftime('%Y-%m-%d')}  |  Liverpool John Moores University",
             align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # ── Overview ───────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.h1("Overview")
    pdf.body(
        "The AMOS MTDE Ingest API accepts telemetry from partner systems via HTTPS POST. "
        "All payloads are JSON encoded. The API authenticates each message and forwards it "
        "to the internal RabbitMQ pipeline where it is processed by the Multi-Tier Decision "
        "Engine across four layers: IoT health scoring, Regional MPC optimisation, Central "
        "fleet scheduling, and AI strategic profiling."
    )

    pdf.h2("Authentication")
    pdf.body(
        "Currently open access. All requests are served over TLS (HTTPS). "
        "Contact the AMOS team to obtain dedicated API credentials for production integration."
    )

    pdf.h2("General Rules")
    for rule in [
        "All requests must set Content-Type: application/json",
        "All timestamps must be ISO 8601 format in UTC e.g. 2026-05-06T03:10:00+00:00",
        "HTTP 202 Accepted = message successfully queued in the pipeline",
        "HTTP 422 Unprocessable Entity = missing required field or wrong data type",
        "HTTP 503 Service Unavailable = RabbitMQ broker not connected",
        "Recommended send rates: sensor 30s | IDB telemetry 30-60s | market signal 5-15min | TTA forecast 1hr",
    ]:
        pdf.bullet(rule)
    pdf.ln(3)

    pdf.h2("Data Flow")
    pdf.code_block(
        "POST /ingest/sensor      -> IoT Layer -> Regional MPC -> Central Fleet LP -> Dashboard\n"
        "POST /ingest/telemetries -> AI Agent Strategic Layer                       -> Dashboard\n"
        "POST /ingest/market      -> Central Fleet LP  +  AI Agent                 -> Dashboard\n"
        "POST /ingest/tta         -> Regional MPC  +  Consensus Layer -> AI Agent  -> Dashboard"
    )

    # ── Endpoint pages ─────────────────────────────────────────────────────────
    for ep in ENDPOINTS:
        pdf.add_page()
        pdf.h2(ep["title"])
        pdf.method_badge(ep["method"], ep["path"])
        pdf.body(ep["description"])

        if ep["request_body"]:
            pdf.h3("Request Body (JSON)")
            pdf.code_block(json.dumps(ep["request_body"], indent=2))
            if ep["fields"]:
                pdf.h3("Field Reference")
                pdf.field_table(ep["fields"])

        pdf.h3("Successful Response")
        code = "202 Accepted" if ep["method"] == "POST" else "200 OK"
        pdf.response_badge(code)
        pdf.code_block(json.dumps(ep["response"], indent=2))

        pdf.h3("Error Responses")
        for code, name, desc in [
            ("422", "Unprocessable Entity", "Missing required field or wrong data type"),
            ("503", "Service Unavailable",  "RabbitMQ broker not connected"),
            ("500", "Internal Server Error", "Failed to publish to broker"),
        ]:
            pdf.bullet(f"HTTP {code} {name}: {desc}")
        pdf.ln(2)

    # ── Quick reference ────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.h1("Quick Reference")

    pdf.h2("curl Examples")
    pdf.code_block(
        f"# Health check\n"
        f"curl {BASE_URL}/health\n\n"
        f"# Sensor reading\n"
        f"curl -X POST {BASE_URL}/ingest/sensor \\\n"
        f'  -H "Content-Type: application/json" \\\n'
        f'  -d \'{{"farm_id":"farm_00","timestamp":"2026-05-06T03:10:00+00:00",\n'
        f'  "panels":[{{"panel_id":"farm_00_panel_00","timestamp":"2026-05-06T03:10:00+00:00",\n'
        f'  "power_kw":42.5,"irradiance_wm2":750.0,"inverter_temp_c":42.5}}]}}\'\n\n'
        f"# IDB telemetry\n"
        f"curl -X POST {BASE_URL}/ingest/telemetries \\\n"
        f'  -H "Content-Type: application/json" \\\n'
        f'  -d \'{{"timestamp":"2026-05-06T03:10:00+00:00","battery_soc_kwh":620.5,\n'
        f'  "battery_soc_max_kwh":1000.0,"battery_temp_c":31.2,"battery_power_kw":45.0,\n'
        f'  "solar_power_kw":380.0,"grid_exchange_kw":-25.0,\n'
        f'  "compressor_vibration_g":0.12,"compressor_load_pct":48.5}}\'\n\n'
        f"# Market signal\n"
        f"curl -X POST {BASE_URL}/ingest/market \\\n"
        f'  -H "Content-Type: application/json" \\\n'
        f'  -d \'{{"timestamp":"2026-05-06T03:10:00+00:00",\n'
        f'  "price_per_kwh":0.2843,"carbon_intensity_gco2_kwh":214.0}}\''
    )

    pdf.h2("Python Example (httpx)")
    pdf.code_block(
        f"import httpx\n"
        f"from datetime import datetime, timezone\n\n"
        f'BASE = "{BASE_URL}"\n'
        f"now  = datetime.now(tz=timezone.utc).isoformat()\n\n"
        f"with httpx.Client() as c:\n"
        f'    c.post(f"{{BASE}}/ingest/sensor", json={{\n'
        f'        "farm_id": "farm_00", "timestamp": now,\n'
        f'        "panels": [{{"panel_id":"farm_00_panel_00","timestamp":now,\n'
        f'                    "power_kw":42.5,"irradiance_wm2":750.0,"inverter_temp_c":42.5}}]\n'
        f"    }})\n"
        f'    c.post(f"{{BASE}}/ingest/telemetries", json={{\n'
        f'        "timestamp": now, "battery_soc_kwh": 620.5,\n'
        f'        "battery_soc_max_kwh": 1000.0, "battery_temp_c": 31.2,\n'
        f'        "battery_power_kw": 45.0, "solar_power_kw": 380.0,\n'
        f'        "grid_exchange_kw": -25.0, "compressor_vibration_g": 0.12,\n'
        f'        "compressor_load_pct": 48.5\n'
        f"    }})\n"
        f'    c.post(f"{{BASE}}/ingest/market", json={{\n'
        f'        "timestamp": now, "price_per_kwh": 0.28,\n'
        f'        "carbon_intensity_gco2_kwh": 214.0\n'
        f"    }})\n"
        f'    # Run send_payloads.py for the full automated sender including TTA'
    )

    pdf.output(OUT_FILE)
    print(f"PDF saved: {OUT_FILE}")


if __name__ == "__main__":
    build_pdf()
