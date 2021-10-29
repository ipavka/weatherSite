from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from .db_postgre import select_last_info, select_avg_temp, select_max_min
from .db_mongo import select_avg_hour, \
    max_min_temp_last_query, select_last_data

app = FastAPI()

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    context = {"request": request,
               'AVG_rp5': select_avg_temp(),
               'max_min_sev_info': select_max_min('sev_info'),
               'max_min_sev_rp5': select_max_min('rp5_ru'),
               'main_data': select_last_info('sev_info'),
               'kras_date': select_last_data(),
               'kras_AVG': select_avg_hour(),
               'kras_max_min': max_min_temp_last_query(),
               }
    return templates.TemplateResponse("index.html", context)


@app.get("/page/{page_name}", response_class=HTMLResponse)
async def page(request: Request, page_name: str):
    data = {
        "page": page_name
    }
    return templates.TemplateResponse("page.html", {"request": request, "data": data})
