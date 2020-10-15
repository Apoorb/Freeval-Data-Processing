import numpy as np
import pandas as pd
import os
import sys
import plotly.express as px
from plotly.offline import plot
from plotly.subplots import make_subplots


# 1.2 Set Global Parameters
path_to_data = r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\June_23_2020"
path_to_grade_data_file = os.path.join(path_to_data, "Processing.gdb")
path_processed_data = os.path.join(path_to_data, "processed_data")

shuyi_dir = (
    r"C:\Users\abibeka\Documents_axb\freeval_pa\grade_data\June_23_2020\processed_data"
    r"\uw_i80_pa"
)
import numpy as np, os, pandas as pd

grade_uw = np.genfromtxt(os.path.join(shuyi_dir, "grades_I80_PA.txt"), delimiter=",")

miles_uw = np.genfromtxt(os.path.join(shuyi_dir, "miles_I80_PA.txt"), delimiter=",")

data_uw = pd.DataFrame({"milepost": miles_uw, "grade_percent": grade_uw})
data_uw_fil = data_uw.query("grade_percent >= -8 & grade_percent < 8")
data_uw_fil.loc[:, "seg_len"] = data_uw_fil.milepost.diff().fillna(0)
data_uw_fil.loc[:, "height"] = (data_uw_fil.seg_len * 5280) * (
    data_uw_fil.grade_percent / 100
)
data_uw_fil.loc[:, "elevation_in_ft"] = data_uw_fil.height.cumsum() + 929
fig = make_subplots(
    rows=2,
    cols=1,
    shared_xaxes=True,
    subplot_titles=(
        f"Grade Profile UW--I-80 and direction E",
        f"Elevation UW--I-80 and direction E",
    ),
)

data_grade_uw = px.line(data_uw_fil, x="milepost", y="grade_percent",)["data"]

data_elevation_uw = px.line(data_uw_fil, x="milepost", y="elevation_in_ft",)["data"]

for dat1, dat2 in zip(data_grade_uw, data_elevation_uw):
    fig.add_trace(dat1, row=1, col=1)
    fig.add_trace(dat2, row=2, col=1)
fig.update_yaxes(title_text="grade (%.)", row=1, col=1)
fig.update_yaxes(title_text="cum_elevation_relative (ft.)", row=2, col=1)
fig.update_xaxes(title_text="Milepost", row=1, col=1)
fig.update_xaxes(title_text="Milepost", row=2, col=1)

plot(fig, filename=os.path.join(path_processed_data, "uw_i_80_grade_profile.html"))
