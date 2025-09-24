import React, { useState, useRef } from "react";
import axios from "axios";
import {
  Upload,
  Download,
  FileSpreadsheet,
  Database,
  Check,
  Loader,
  Layers,
  RefreshCw,
} from "lucide-react";

export default function App() {
  const [activeTab, setActiveTab] = useState("ddl");
  const [file, setFile] = useState(null);
  const [bronzeFile, setBronzeFile] = useState(null);
  const [silverFile, setSilverFile] = useState(null);
  const [autoSilver, setAutoSilver] = useState(true);
  const [target, setTarget] = useState("databricks");
  const [result, setResult] = useState(null);
  const [mappingResult, setMappingResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const fileInputRef = useRef(null);
  const bronzeInputRef = useRef(null);
  const silverInputRef = useRef(null);

  // --- DDL Generator Logic ---
  const handleSubmitDDL = async (e) => {
    e.preventDefault();
    if (!file) return alert("Please upload an Excel file");
    const formData = new FormData();
    formData.append("file", file);
    formData.append("target", target);
    try {
      setLoading(true);
      const res = await axios.post(
        "http://127.0.0.1:8000/api/generate-schema",
        formData,
        { headers: { "Content-Type": "multipart/form-data" } }
      );
      setResult(res.data);
    } catch (err) {
      console.error(err);
      alert(
        "Error generating schema: " +
          (err.response?.data?.message || err.message || "Unexpected error")
      );
    } finally {
      setLoading(false);
    }
  };

  // --- Bronze→Silver Mapping Logic ---
  const handleSubmitMapping = async (e) => {
    e.preventDefault();
    if (!bronzeFile) return alert("Please upload Bronze file");
    if (!autoSilver && !silverFile)
      return alert("Please upload Silver file or enable Auto-create Silver");
    const formData = new FormData();
    formData.append("bronze_file", bronzeFile);
    formData.append("bronze_name", "bronze_table");
    formData.append("bronze_filename", bronzeFile.name);
    if (!autoSilver) {
      formData.append("silver_file", silverFile);
      formData.append("silver_name", "silver_table");
      formData.append("silver_filename", silverFile.name);
    } else {
      formData.append("silver_name", "auto_silver_table");
    }
    try {
      setLoading(true);
      const res = await axios.post(
        "http://127.0.0.1:8000/api/map_bronze_to_silver/",
        formData,
        { headers: { "Content-Type": "multipart/form-data" } }
      );
      setMappingResult(res.data);
    } catch (err) {
      console.error(err);
      alert(
        "Error mapping tables: " +
          (err.response?.data?.message || err.message || "Unexpected error")
      );
    } finally {
      setLoading(false);
    }
  };

  // --- Drag & Drop Logic ---
  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") setDragActive(true);
    else if (e.type === "dragleave") setDragActive(false);
  };

  const handleDrop = (e, fileType) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    const droppedFile = e.dataTransfer.files[0];
    if (!droppedFile) return;
    if (fileType === "ddl") setFile(droppedFile);
    else if (fileType === "bronze") setBronzeFile(droppedFile);
    else if (fileType === "silver") setSilverFile(droppedFile);
  };

  // --- Download / Copy ---
  const downloadDDL = () => {
    if (!result?.ddl) return;
    const blob = new Blob([result.ddl], { type: "text/sql" });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "schema.sql";
    a.click();
  };

  const downloadMappingDDL = () => {
    if (!mappingResult?.ddl) return;
    const blob = new Blob([mappingResult.ddl], { type: "text/sql" });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "mapping_schema.sql";
    a.click();
  };

  const downloadMappingExcel = () => {
    if (!mappingResult?.mapping_file) return;
    window.open(`http://127.0.0.1:8000/api/download_mapping/${mappingResult.mapping_file}`, "_blank");
  };

  // --- Reset ---
  const resetForm = () => {
    setFile(null);
    setResult(null);
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const resetMappingForm = () => {
    setBronzeFile(null);
    setSilverFile(null);
    setMappingResult(null);
    setAutoSilver(true);
    if (bronzeInputRef.current) bronzeInputRef.current.value = "";
    if (silverInputRef.current) silverInputRef.current.value = "";
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-blue-50">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center space-x-3">
          <div className="bg-blue-600 p-2 rounded-lg">
            <Database className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Data Schema Tool</h1>
            <p className="text-sm text-gray-500">Generate DDL and map Bronze→Silver schemas</p>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-6xl mx-auto p-6">
        {/* Tabs */}
        <div className="flex border-b mb-6">
          <button
            onClick={() => setActiveTab("ddl")}
            className={`px-4 py-2 font-medium ${
              activeTab === "ddl" ? "border-b-2 border-blue-600 text-blue-600" : "text-gray-500"
            }`}
          >
            DDL Generator
          </button>
          <button
            onClick={() => setActiveTab("mapping")}
            className={`px-4 py-2 font-medium ${
              activeTab === "mapping" ? "border-b-2 border-blue-600 text-blue-600" : "text-gray-500"
            }`}
          >
            Bronze→Silver Mapping
          </button>
        </div>

        {/* DDL Tab */}
        {activeTab === "ddl" ? (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Upload */}
            <div className="lg:col-span-1 bg-white rounded-xl shadow-sm border p-6">
              <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
                <Upload className="w-5 h-5 mr-2 text-blue-600" />
                Upload & Configure
              </h2>
              <div
                className={`relative border-2 border-dashed rounded-lg p-6 text-center transition-colors ${
                  dragActive
                    ? "border-blue-500 bg-blue-50"
                    : file
                    ? "border-green-500 bg-green-50"
                    : "border-gray-300 hover:border-gray-400"
                }`}
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={(e) => handleDrop(e, "ddl")}
              >
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".xlsx,.xls,.csv"
                  onChange={(e) => setFile(e.target.files[0])}
                  className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                />
                {file ? (
                  <div className="flex flex-col items-center space-y-2">
                    <Check className="w-8 h-8 text-green-600" />
                    <p className="text-sm font-medium text-green-700">{file.name}</p>
                  </div>
                ) : (
                  <div className="flex flex-col items-center space-y-2">
                    <FileSpreadsheet className="w-8 h-8 text-gray-400" />
                    <p className="text-sm text-gray-700">Drop your Excel file here</p>
                  </div>
                )}
              </div>
              <div className="mt-4">
                <label className="block text-sm font-medium text-gray-700 mb-2">Target Database</label>
                <select
                  value={target}
                  onChange={(e) => setTarget(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  <option value="databricks">Databricks (Delta Lake)</option>
                  <option value="snowflake">Snowflake</option>
                </select>
              </div>
              <div className="flex space-x-3 mt-4">
                <button
                  onClick={handleSubmitDDL}
                  disabled={!file || loading}
                  className="flex-1 bg-blue-600 text-white rounded-lg py-2 px-4 hover:bg-blue-700 disabled:bg-gray-300 flex items-center justify-center space-x-2"
                >
                  {loading ? (
                    <>
                      <Loader className="w-4 h-4 animate-spin" />
                      <span>Processing...</span>
                    </>
                  ) : (
                    <>
                      <Database className="w-4 h-4" />
                      <span>Generate DDL</span>
                    </>
                  )}
                </button>
                {(file || result) && (
                  <button
                    onClick={resetForm}
                    disabled={loading}
                    className="px-4 py-2 text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50"
                  >
                    Reset
                  </button>
                )}
              </div>
            </div>

            {/* Results */}
            <div className="lg:col-span-2">
              {result ? (
                <div className="space-y-6">
                  {/* Table Metadata */}
                  <div className="bg-white rounded-xl shadow-sm border p-6">
                    <h3 className="text-lg font-semibold text-gray-900 mb-4">📝 Table Metadata ({target})</h3>
                    <div className="overflow-x-auto">
                      <table className="min-w-full border border-gray-200 rounded-lg">
                        <thead>
                          <tr className="bg-gray-100 text-gray-700">
                            <th className="px-4 py-2 text-left">Timestamp</th>
                            <th className="px-4 py-2 text-left">Table Name</th>
                            <th className="px-4 py-2 text-left">Batch ID</th>
                            <th className="px-4 py-2 text-left">Rows Processed</th>
                            <th className="px-4 py-2 text-left">Processing Time</th>
                          </tr>
                        </thead>
                        <tbody>
                          {result.history
                            ?.filter((entry) => entry.target === target)
                            .slice(-1)
                            .map((entry, idx) => (
                              <tr key={idx} className="border-t border-gray-200">
                                <td className="px-4 py-2 font-mono">{entry.timestamp}</td>
                                <td className="px-4 py-2">{entry.table_name}</td>
                                <td className="px-4 py-2 font-mono">{entry.batch_id}</td>
                                <td className="px-4 py-2">{entry.rows_processed}</td>
                                <td className="px-4 py-2">{entry.processing_time}</td>
                              </tr>
                            ))}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  {/* Generated DDL */}
                  <div className="bg-white rounded-xl shadow-sm border p-6">
                    <div className="flex items-center justify-between mb-4">
                      <h3 className="text-lg font-semibold text-gray-900 flex items-center">
                        🔧 Generated DDL ({target})
                      </h3>
                      <div className="flex space-x-2">
                        <button
                          onClick={downloadDDL}
                          className="flex items-center space-x-2 bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700"
                        >
                          <Download className="w-4 h-4" />
                          <span>Download SQL</span>
                        </button>
                        <button
                          onClick={() => navigator.clipboard.writeText(result.ddl)}
                          className="flex items-center space-x-2 bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700"
                        >
                          <Download className="w-4 h-4" />
                          <span>Copy Query</span>
                        </button>
                      </div>
                    </div>
                    <div className="bg-slate-900 rounded-lg p-4 overflow-x-auto">
                      <pre className="text-sm text-green-400 font-mono">{result.ddl}</pre>
                    </div>
                  </div>

                  {/* Table Upload History (Databricks) */}
                  {result.history && result.history.filter(entry => entry.target === "databricks").length > 0 && (
                    <div className="bg-white rounded-xl shadow-sm border p-6">
                      <h3 className="text-lg font-semibold text-gray-900 mb-4">🕒 Table Upload History (Databricks)</h3>
                      <div className="overflow-x-auto">
                        <table className="min-w-full border border-gray-200 rounded-lg">
                          <thead>
                            <tr className="bg-gray-100 text-gray-700">
                              <th className="px-4 py-2 text-left">Timestamp</th>
                              <th className="px-4 py-2 text-left">Table Name</th>
                              <th className="px-4 py-2 text-left">Batch ID</th>
                              <th className="px-4 py-2 text-left">Rows Processed</th>
                              <th className="px-4 py-2 text-left">Processing Time</th>
                            </tr>
                          </thead>
                          <tbody>
                            {result.history
                              .filter((entry) => entry.target === "databricks")
                              .map((entry, idx) => (
                                <tr key={idx} className="border-t border-gray-200">
                                  <td className="px-4 py-2 font-mono">{entry.timestamp}</td>
                                  <td className="px-4 py-2">{entry.table_name}</td>
                                  <td className="px-4 py-2 font-mono">{entry.batch_id}</td>
                                  <td className="px-4 py-2">{entry.rows_processed}</td>
                                  <td className="px-4 py-2">{entry.processing_time}</td>
                                </tr>
                              ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}

                  {/* Table Upload History (Snowflake) */}
                  {result.history && result.history.filter(entry => entry.target === "snowflake").length > 0 && (
                    <div className="bg-white rounded-xl shadow-sm border p-6">
                      <h3 className="text-lg font-semibold text-gray-900 mb-4">🕒 Table Upload History (Snowflake)</h3>
                      <div className="overflow-x-auto">
                        <table className="min-w-full border border-gray-200 rounded-lg">
                          <thead>
                            <tr className="bg-gray-100 text-gray-700">
                              <th className="px-4 py-2 text-left">Timestamp</th>
                              <th className="px-4 py-2 text-left">Table Name</th>
                              <th className="px-4 py-2 text-left">Batch ID</th>
                              <th className="px-4 py-2 text-left">Rows Processed</th>
                              <th className="px-4 py-2 text-left">Processing Time</th>
                            </tr>
                          </thead>
                          <tbody>
                            {result.history
                              .filter((entry) => entry.target === "snowflake")
                              .map((entry, idx) => (
                                <tr key={idx} className="border-t border-gray-200">
                                  <td className="px-4 py-2 font-mono">{entry.timestamp}</td>
                                  <td className="px-4 py-2">{entry.table_name}</td>
                                  <td className="px-4 py-2 font-mono">{entry.batch_id}</td>
                                  <td className="px-4 py-2">{entry.rows_processed}</td>
                                  <td className="px-4 py-2">{entry.processing_time}</td>
                                </tr>
                              ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <div className="bg-white rounded-xl shadow-sm border p-12 text-center">
                  <Database className="w-16 h-16 text-gray-400 mx-auto mb-4" />
                  <h3 className="text-lg font-medium text-gray-900 mb-2">Ready to Generate Schema</h3>
                  <p className="text-gray-500 max-w-md mx-auto">
                    Upload an Excel file and select your target database to generate DDL with metadata.
                  </p>
                </div>
              )}
            </div>
          </div>
        ) : (
          // --- Mapping Tab ---
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Upload */}
            <div className="lg:col-span-1 bg-white rounded-xl shadow-sm border p-6">
              <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
                <Layers className="w-5 h-5 mr-2 text-blue-600" />
                Bronze→Silver Mapping
              </h2>
              {/* Auto-create Silver Checkbox */}
              <label className="flex items-center space-x-2 mb-4">
                <input
                  type="checkbox"
                  checked={autoSilver}
                  onChange={(e) => setAutoSilver(e.target.checked)}
                  className="h-4 w-4 text-blue-600"
                />
                <span className="text-gray-700 text-sm">Auto-create Silver table</span>
              </label>
              {/* Bronze Upload */}
              <label className="block text-sm font-medium text-gray-700 mb-2">Bronze Table</label>
              <div
                className={`relative border-2 border-dashed rounded-lg p-4 text-center transition-colors ${
                  dragActive ? "border-blue-500 bg-blue-50" : bronzeFile ? "border-green-500 bg-green-50" : "border-gray-300 hover:border-gray-400"
                }`}
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={(e) => handleDrop(e, "bronze")}
              >
                <input
                  ref={bronzeInputRef}
                  type="file"
                  accept=".xlsx,.xls,.csv"
                  onChange={(e) => setBronzeFile(e.target.files[0])}
                  className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                />
                {bronzeFile ? (
                  <p className="text-green-700 font-medium">{bronzeFile.name}</p>
                ) : (
                  <p className="text-gray-500">Drop Bronze file here</p>
                )}
              </div>
              {/* Silver Upload (only if autoSilver=false) */}
              {!autoSilver && (
                <>
                  <label className="block text-sm font-medium text-gray-700 mt-4 mb-2">Silver Table</label>
                  <div
                    className={`relative border-2 border-dashed rounded-lg p-4 text-center transition-colors ${
                      dragActive ? "border-blue-500 bg-blue-50" : silverFile ? "border-green-500 bg-green-50" : "border-gray-300 hover:border-gray-400"
                    }`}
                    onDragEnter={handleDrag}
                    onDragLeave={handleDrag}
                    onDragOver={handleDrag}
                    onDrop={(e) => handleDrop(e, "silver")}
                  >
                    <input
                      ref={silverInputRef}
                      type="file"
                      accept=".xlsx,.xls,.csv"
                      onChange={(e) => setSilverFile(e.target.files[0])}
                      className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                    />
                    {silverFile ? <p className="text-green-700 font-medium">{silverFile.name}</p> : <p className="text-gray-500">Drop Silver file here</p>}
                  </div>
                </>
              )}
              <div className="flex space-x-3 mt-4">
                <button
                  onClick={handleSubmitMapping}
                  disabled={!bronzeFile || (!autoSilver && !silverFile) || loading}
                  className="flex-1 bg-blue-600 text-white rounded-lg py-2 px-4 hover:bg-blue-700 disabled:bg-gray-300 flex items-center justify-center space-x-2"
                >
                  {loading ? (
                    <>
                      <Loader className="w-4 h-4 animate-spin" />
                      <span>Processing...</span>
                    </>
                  ) : (
                    <>
                      <Layers className="w-4 h-4" />
                      <span>Generate Mapping</span>
                    </>
                  )}
                </button>
                {(bronzeFile || silverFile || mappingResult) && (
                  <button
                    onClick={resetMappingForm}
                    disabled={loading}
                    className="px-4 py-2 text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50"
                  >
                    Reset
                  </button>
                )}
              </div>
            </div>

            {/* Mapping Results */}
            <div className="lg:col-span-2">
              {mappingResult ? (
                <div className="space-y-6">
                  <div className="bg-white rounded-xl shadow-sm border p-6">
                    <div className="flex items-center justify-between mb-4">
                      <h3 className="text-lg font-semibold text-gray-900 flex items-center">
                        🔧 Generated Mapping DDL
                      </h3>
                      <div className="flex space-x-2">
                        <button
                          onClick={downloadMappingDDL}
                          className="flex items-center space-x-2 bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700"
                        >
                          <Download className="w-4 h-4" />
                          <span>Download SQL</span>
                        </button>
                        <button
                          onClick={() => navigator.clipboard.writeText(mappingResult.ddl)}
                          className="flex items-center space-x-2 bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700"
                        >
                          <Download className="w-4 h-4" />
                          <span>Copy Query</span>
                        </button>
                        <button
                          onClick={downloadMappingExcel}
                          className="flex items-center space-x-2 bg-purple-600 text-white px-4 py-2 rounded-lg hover:bg-purple-700"
                        >
                          <Download className="w-4 h-4" />
                          <span>Download Mapping Excel</span>
                        </button>
                      </div>
                    </div>
                    <div className="bg-slate-900 rounded-lg p-4 overflow-x-auto">
                      <pre className="text-sm text-green-400 font-mono">{mappingResult.ddl}</pre>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="bg-white rounded-xl shadow-sm border p-12 text-center">
                  <Layers className="w-16 h-16 text-gray-400 mx-auto mb-4" />
                  <h3 className="text-lg font-medium text-gray-900 mb-2">Ready to Map Tables</h3>
                  <p className="text-gray-500 max-w-md mx-auto">
                    Upload Bronze Excel file and optionally a Silver Excel file. If "Auto-create Silver table" is checked, Silver schema will be generated automatically.
                  </p>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
