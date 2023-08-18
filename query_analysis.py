import streamlit as st
import matplotlib.pyplot as plt
from typing import NoReturn
from dags.helpers.bigquery_helper import BigQueryDataManager
from dags.helpers.constants import PROJECT_ID, DATASET_NAME, TABLE_NAME


st.set_page_config(page_title='Server Report Analysis')
st.title('Server Report Analysis')
st.header("Chart of Event Flag Counts")

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)
custom_footer = """
            <div style = "position: fixed;bottom: 20px;width: 100%;">
                Made with ❤️ by <a href="https://www.linkedin.com/in/tamunopriye-dagogo-george-191175167/">Tamunopriye</a>
            </div>
            """
st.write(custom_footer, unsafe_allow_html=True)




def main() -> NoReturn:
    """Main fuction to execute runs

    Returns:
        NoReturn: Returns no value
    """

    query = f"""
        SELECT event_flag, count(*) FROM {DATASET_NAME}.{TABLE_NAME}
        group by Event_flag;
    """

    bigquery_client =  BigQueryDataManager(
                PROJECT_ID, DATASET_NAME, TABLE_NAME
            )
    results = bigquery_client.run_query(query)

    data = {}
    for row in results:
        data[row[0]] = row[1]

    fig, ax = plt.subplots(facecolor='#C4D0B3', figsize=(8, 8))
    ax.bar(data.keys(), data.values(), color='#3D6303', align='center')

    # Customize the chart
    ax.set_ylabel('Occurrences', fontweight='bold')
    ax.set_facecolor('black')

    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha="right")

    # Display the chart in Streamlit
    st.pyplot(fig)



if __name__ == '__main__':
    main()