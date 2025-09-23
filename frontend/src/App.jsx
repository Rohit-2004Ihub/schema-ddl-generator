import React, { useState, useRef } from "react";
import axios from "axios";
import { Upload, Download, FileSpreadsheet, Database, Check, Loader } from "lucide-react";

export default function App() {
  const [file, setFile] = useState(null);
  const [target, setTarget] = useState("databricks");
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const fileInputRef = useRef(null);

  const handleSubmit = async (e) => {
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
          (err.response?.data?.message ||
            err.message ||
            "An unexpected error occurred. Please try again.")
      );
    } finally {
      setLoading(false);
    }
  };

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    const droppedFile = e.dataTransfer.files[0];
    if (droppedFile && (droppedFile.name.endsWith(".xlsx") || droppedFile.name.endsWith(".xls") || droppedFile.name.endsWith(".csv"))) {
      setFile(droppedFile);
    } else {
      alert("Please drop a valid Excel file (.xlsx, .xls, .csv)");
    }
  };

  const downloadDDL = () => {
    if (!result?.ddl) return;
    const blob = new Blob([result.ddl], { type: "text/sql" });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "schema.sql";
    a.click();
  };

  const resetForm = () => {
    setFile(null);
    setResult(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-blue-50">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-6xl mx-auto px-6 py-4">
          <div className="flex items-center space-x-3">
            <div className="bg-blue-600 p-2 rounded-lg">
              <Database className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">DDL Generator</h1>
              <p className="text-sm text-gray-500">Convert Excel files to database schemas</p>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-6xl mx-auto p-6">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Upload Section */}
          <div className="lg:col-span-1">
            <div className="bg-white rounded-xl shadow-sm border p-6">
              <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
                <Upload className="w-5 h-5 mr-2 text-blue-600" />
                Upload & Configure
              </h2>
              <div className="space-y-4">
                {/* File Upload */}
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
                  onDrop={handleDrop}
                >
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept=".xlsx,.xls,.csv"
                    onChange={(e) => {
                      const selectedFile = e.target.files[0];
                      if (selectedFile && (selectedFile.name.endsWith(".xlsx") || selectedFile.name.endsWith(".xls") || selectedFile.name.endsWith(".csv"))) {
                        setFile(selectedFile);
                      } else {
                        alert("Please select a valid Excel file (.xlsx, .xls, .csv)");
                      }
                    }}
                    className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                  />
                  {file ? (
                    <div className="flex flex-col items-center space-y-2">
                      <Check className="w-8 h-8 text-green-600" />
                      <p className="text-sm font-medium text-green-700">{file.name}</p>
                      <p className="text-xs text-green-600">Ready to process</p>
                    </div>
                  ) : (
                    <div className="flex flex-col items-center space-y-2">
                      <FileSpreadsheet className="w-8 h-8 text-gray-400" />
                      <p className="text-sm font-medium text-gray-700">Drop your Excel file here</p>
                      <p className="text-xs text-gray-500">or click to browse</p>
                      <p className="text-xs text-gray-400">.xlsx, .xls, .csv files only</p>
                    </div>
                  )}
                </div>

                {/* Database Target */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Target Database</label>
                  <select
                    value={target}
                    onChange={(e) => setTarget(e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  >
                    <option value="databricks">🔶 Databricks (Delta Lake)</option>
                    <option value="snowflake">❄️ Snowflake</option>
                  </select>
                </div>

                {/* Actions */}
                <div className="flex space-x-3">
                  <button
                    onClick={handleSubmit}
                    disabled={!file || loading}
                    className="flex-1 bg-blue-600 text-white rounded-lg py-2 px-4 hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors flex items-center justify-center space-x-2"
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
                      className="px-4 py-2 text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      Reset
                    </button>
                  )}
                </div>
              </div>
            </div>
          </div>

          {/* Results Section */}
          <div className="lg:col-span-2">
            {result ? (
              <div className="space-y-6">
                {/* Table Metadata for latest uploaded file (Filtered by target) */}
                <div className="bg-white rounded-xl shadow-sm border p-6">
                  <h3 className="text-lg font-semibold text-gray-900 mb-4">
                    📝 Table Info ({target})
                  </h3>
                  <div className="overflow-x-auto">
                    <table className="min-w-full border border-gray-200 rounded-lg">
                      <thead>
                        <tr className="bg-gray-100 text-gray-700">
                          <th className="px-4 py-2 text-left">Attribute</th>
                          <th className="px-4 py-2 text-left">Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        {result.history &&
                          result.history
                            .filter((entry) => entry.target === target)
                            .slice(-1)
                            .map((latest, idx) =>
                              Object.entries(latest).map(([key, value]) => (
                                <tr key={`${idx}-${key}`} className="border-t border-gray-200">
                                  <td className="px-4 py-2 font-medium">{key.replace(/_/g, " ")}</td>
                                  <td className="px-4 py-2 font-mono">{value}</td>
                                </tr>
                              ))
                            )}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* DDL Output */}
                <div className="bg-white rounded-xl shadow-sm border p-6">
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-semibold text-gray-900 flex items-center">
                      🔧 Generated DDL ({target})
                    </h3>
                    <button
                      onClick={downloadDDL}
                      className="flex items-center space-x-2 bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors"
                    >
                      <Download className="w-4 h-4" />
                      <span>Download SQL</span>
                    </button>
                  </div>
                  <div className="bg-slate-900 rounded-lg p-4 overflow-x-auto">
                    <pre className="text-sm text-green-400 font-mono">{result.ddl}</pre>
                  </div>
                </div>

                {/* Table History Filtered by Target */}
                {result.history && result.history.length > 0 && (
                  <div className="bg-white rounded-xl shadow-sm border p-6 mt-6">
                    <h3 className="text-lg font-semibold text-gray-900 mb-4">
                      🕒 Table Upload History ({target})
                    </h3>
                    <div className="overflow-x-auto">
                      <table className="min-w-full border border-gray-200 rounded-lg">
                        <thead>
                          <tr className="bg-gray-100 text-gray-700">
                            <th className="px-4 py-2 text-left">Timestamp</th>
                            <th className="px-4 py-2 text-left">Table Name</th>
                            <th className="px-4 py-2 text-left">Target DB</th>
                            <th className="px-4 py-2 text-left">Batch ID</th>
                            <th className="px-4 py-2 text-left">Rows Processed</th>
                            <th className="px-4 py-2 text-left">Processing Time</th>
                          </tr>
                        </thead>
                        <tbody>
                          {result.history
                            .filter((entry) => entry.target === target)
                            .map((entry, idx) => (
                              <tr key={idx} className="border-t border-gray-200">
                                <td className="px-4 py-2 font-mono">{entry.timestamp}</td>
                                <td className="px-4 py-2">{entry.table_name}</td>
                                <td className="px-4 py-2">{entry.target}</td>
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
                <div className="flex flex-col items-center space-y-4">
                  <div className="w-16 h-16 bg-gray-100 rounded-full flex items-center justify-center">
                    <Database className="w-8 h-8 text-gray-400" />
                  </div>
                  <div>
                    <h3 className="text-lg font-medium text-gray-900 mb-2">Ready to Generate Schema</h3>
                    <p className="text-gray-500 max-w-md">
                      Upload an Excel file and select your target database to automatically generate DDL statements with metadata.
                    </p>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
