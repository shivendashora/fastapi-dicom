from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import os
import pydicom
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import requests
from urllib.parse import urlparse

# ✅ Database Configuration
DATABASE_URL = "postgresql://dicomuser:123@localhost:5432/dicomdb"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ✅ Extended DICOM Model
class DicomFile(Base):
    __tablename__ = 'dicom_files'

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, unique=True, index=True)
    patient_id = Column(String)
    patient_name = Column(String)
    patient_birth_date = Column(String)
    patient_sex = Column(String)
    patient_age = Column(String)
    patient_weight = Column(String)
    patient_address = Column(String)
    study_date = Column(String)
    study_time = Column(String)
    study_id = Column(String)
    study_modality = Column(String)
    study_description = Column(String)
    series_date = Column(String)
    series_time = Column(String)
    series_description = Column(String)
    file_path = Column(String)

# ✅ Create tables
Base.metadata.create_all(bind=engine)

# ✅ FastAPI App
app = FastAPI()

# ✅ Create Upload Directory
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ✅ Function to validate DICOM file
def is_valid_dicom(file_path: str) -> bool:
    try:
        pydicom.dcmread(file_path)
        return True
    except Exception as e:
        print(f"❌ Invalid DICOM file: {e}")
        return False

# ✅ Function to parse and store DICOM file
def parse_and_store_dicom(file_path: str):
    try:
        dicom_data = pydicom.dcmread(file_path)

        # Extracting metadata
        patient_id = getattr(dicom_data, "PatientID", "Unknown")
        
        patient_name = getattr(dicom_data, "PatientName", "Unknown")
        if hasattr(patient_name, 'family_name') and hasattr(patient_name, 'given_name'):
            patient_name = f"{patient_name.family_name}, {patient_name.given_name}"
        else:
            patient_name = str(patient_name)

        patient_birth_date = getattr(dicom_data, "PatientBirthDate", "")
        if patient_birth_date:
            patient_birth_date = datetime.strptime(str(patient_birth_date), "%Y%m%d").strftime("%d-%b-%Y")

        patient_sex = getattr(dicom_data, "PatientSex", "Unknown")
        patient_address = getattr(dicom_data, "PatientAddress", "Unknown")
        patient_weight = str(getattr(dicom_data, "PatientWeight", "Unknown"))
        patient_age = str(getattr(dicom_data, "PatientAge", "Unknown"))

        study_date = getattr(dicom_data, "StudyDate", "")
        if study_date:
            study_date = datetime.strptime(str(study_date), "%Y%m%d").strftime("%d-%b-%Y")

        study_time = getattr(dicom_data, "StudyTime", "Unknown")
        study_id = getattr(dicom_data, "StudyID", "Unknown")
        
        # ✅ Fix: Access Modality tag correctly
        study_modality = getattr(dicom_data, "Modality", "Unknown")
        if study_modality == "Unknown":
            if (0x0008, 0x0060) in dicom_data:
                study_modality = dicom_data[0x0008, 0x0060].value

        study_description = getattr(dicom_data, "StudyDescription", "Unknown")

        series_date = getattr(dicom_data, "SeriesDate", "")
        if series_date:
            series_date = datetime.strptime(str(series_date), "%Y%m%d").strftime("%d-%b-%Y")

        series_time = getattr(dicom_data, "SeriesTime", "Unknown")
        series_description = getattr(dicom_data, "SeriesDescription", "Unknown")

        # ✅ Save metadata to DB
        db = SessionLocal()
        dicom_entry = DicomFile(
            filename=os.path.basename(file_path),
            patient_id=patient_id,
            patient_name=patient_name,
            patient_birth_date=patient_birth_date,
            patient_sex=patient_sex,
            patient_age=patient_age,
            patient_weight=patient_weight,
            patient_address=patient_address,
            study_date=study_date,
            study_time=study_time,
            study_id=study_id,
            study_modality=study_modality,
            study_description=study_description,
            series_date=series_date,
            series_time=series_time,
            series_description=series_description,
            file_path=file_path,
        )
        db.add(dicom_entry)
        db.commit()
        db.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# ✅ Function to reset ID sequence
def reset_id_sequence():
    db = SessionLocal()
    try:
        # Delete all records
        db.query(DicomFile).delete()
        db.commit()
        
        # Reset the ID sequence to 1
        db.execute(text("ALTER SEQUENCE dicom_files_id_seq RESTART WITH 1"))
        db.commit()
        print("✅ ID sequence reset to 1")
    except Exception as e:
        db.rollback()
        print(f"❌ Error resetting ID sequence: {e}")
    finally:
        db.close()

# ✅ Endpoint 1: Upload and parse DICOM file (local file upload)
@app.post("/upload/")
async def upload_dicom(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".dcm"):
        raise HTTPException(status_code=400, detail="Invalid file format. Only DICOM files are allowed")

    file_path = os.path.join(UPLOAD_DIR, file.filename)

    file_content = await file.read()
    with open(file_path, "wb") as f:
        f.write(file_content)

    if not is_valid_dicom(file_path):
        os.remove(file_path)
        raise HTTPException(status_code=400, detail="Invalid DICOM file")

    parse_and_store_dicom(file_path)

    return JSONResponse(
        status_code=200,
        content={"filename": file.filename, "message": "File uploaded and metadata stored successfully"}
    )

# ✅ Endpoint 2: Upload and parse DICOM file (remote URL)
@app.post("/upload-from-url/")
async def upload_dicom_from_url(url: str):
    try:
        # Download the file from the URL
        response = requests.get(url)
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to download the file")

        # Extract the filename from the URL
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        if not filename.lower().endswith(".dcm"):
            raise HTTPException(status_code=400, detail="Invalid file format. Only DICOM files are allowed")

        file_path = os.path.join(UPLOAD_DIR, filename)

        # Save the downloaded file
        with open(file_path, "wb") as f:
            f.write(response.content)

        if not is_valid_dicom(file_path):
            os.remove(file_path)
            raise HTTPException(status_code=400, detail="Invalid DICOM file")

        parse_and_store_dicom(file_path)

        return JSONResponse(
            status_code=200,
            content={"filename": filename, "message": "File uploaded and metadata stored successfully"}
        )
    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ✅ Endpoint 3: Get metadata by filename
@app.get("/metadata/{filename}")
def get_metadata(filename: str):
    db = SessionLocal()
    file_data = db.query(DicomFile).filter(DicomFile.filename == filename).first()
    db.close()

    if not file_data:
        raise HTTPException(status_code=404, detail="File not found")

    return {
        "filename": file_data.filename,
        "patient_id": file_data.patient_id,
        "patient_name": file_data.patient_name,
        "study_date": file_data.study_date,
        "modality": file_data.study_modality,
    }

# ✅ Endpoint 4: List all uploaded DICOM files
@app.get("/files/")
def list_files():
    db = SessionLocal()
    files = db.query(DicomFile).all()
    db.close()

    return [
        {
            "filename": file.filename,
            "patient_name": file.patient_name,
            "study_date": file.study_date,
            "modality": file.study_modality,
            "series_description": file.series_description,
        }
        for file in files
    ]

# ✅ Endpoint 5: Delete all records and reset ID sequence
@app.delete("/delete-all/")
def delete_all_records():
    reset_id_sequence()
    return {"message": "All records deleted and ID sequence reset to 1"}

# ✅ Start FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)