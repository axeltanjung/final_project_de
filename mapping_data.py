import datetime

def parse_data(data):
    # Define dictionaries to map province and district names to IDs
    province_mapping = {
        "province_id": "kode_prov",
        "province_name": "nama_prov",
        "district_id": "kode_kab",
        "district_name": "nama_kab",
    }
    
    # Define the current date
    current_date = datetime.datetime.strptime(data['tanggal'], '%Y-%m-%d').date()

    # Define the status details for the Case table
    status_details = {
        'SUSPECT': {
            'status_name': 'suspect',
            'status_detail': {
                'diisolasi': data['suspect_diisolasi'],
                'discarded': data['suspect_discarded'],
                'meninggal': data['suspect_meninggal']
            }
        },
        'CLOSECONTACT': {
            'status_name': 'closecontact',
            'status_detail': {
                'dikarantina': data['closecontact_dikarantina'],
                'discarded': data['closecontact_discarded'],
                'meninggal': data['closecontact_meninggal']
            }
        },
        'PROBABLE': {
            'status_name': 'probable',
            'status_detail': {
                'diisolasi': data['probable_diisolasi'],
                'discarded': data['probable_discarded'],
                'meninggal': data['probable_meninggal']
            }
        },
        'CONFIRMATION': {
            'status_name': 'confirmation',
            'status_detail': {
                'meninggal': data['confirmation_meninggal'],
                'sembuh': data['confirmation_sembuh']
            }
        }
    }

    # Parse the data into the tables
    province_id = province_mapping[data['nama_prov']]
    district_id = district_mapping[data['nama_kab']]
    
    case_data = []
    for status, details in status_details.items():
        case_data.append({
            'status_name': details['status_name'],
            'status_detail': json.dumps(details['status_detail'])
        })

    return {
        'province_id': province_id,
        'district_id': district_id,
        'current_date': current_date,
        'case_data': case_data
    }
