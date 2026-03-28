import 'dart:convert';
import 'package:http/http.dart' as http;
import '../models/student.dart';

class ApiService {
  static const String baseUrl = 'http://localhost:8000';
  static String? _token;

  static void setToken(String token) => _token = token;
  static String? get token => _token;

  static Map<String, String> get _headers => {
    'Content-Type': 'application/json',
    if (_token != null) 'Authorization': 'Bearer $_token',
  };

  // --- AUTH ---
  static Future<String> login(String email, String password) async {
    final res = await http.post(
      Uri.parse('$baseUrl/auth/login'),
      headers: _headers,
      body: jsonEncode({'email': email, 'password': password}),
    );
    final data = jsonDecode(res.body);
    if (res.statusCode == 200) {
      return data['token'] as String;
    }
    throw Exception(data['error']?['message'] ?? 'Login fallito');
  }

  static Future<String> register(String email, String password) async {
    final res = await http.post(
      Uri.parse('$baseUrl/auth/register'),
      headers: _headers,
      body: jsonEncode({'email': email, 'password': password}),
    );
    final data = jsonDecode(res.body);
    if (res.statusCode == 200) {
      return data['token'] as String;
    }
    throw Exception(data['error']?['message'] ?? 'Registrazione fallita');
  }

  static Future<void> updateProfile(List<String> subjects, String style) async {
    final res = await http.put(
      Uri.parse('$baseUrl/me/profile'),
      headers: _headers,
      body: jsonEncode({'study_subjects': subjects, 'learning_style': style}),
    );
    if (res.statusCode != 200) {
      final data = jsonDecode(res.body);
      throw Exception(data['error']?['message'] ?? 'Errore profilo');
    }
  }

  // --- SWIPE ---
  static Future<List<Student>> getRecommendations() async {
    final res = await http.post(
      Uri.parse('$baseUrl/matches/recommendations'),
      headers: _headers,
    );
    if (res.statusCode == 200) {
      final List data = jsonDecode(res.body);
      return data.map((e) => Student.fromJson(e)).toList();
    }
    throw Exception('Impossibile caricare i suggerimenti');
  }

  static Future<bool> swipe(String targetId, bool like) async {
    final res = await http.post(
      Uri.parse('$baseUrl/swipe'),
      headers: _headers,
      body: jsonEncode({
        'target_user_id': targetId,
        'direction': like ? 'like' : 'dislike',
      }),
    );
    final data = jsonDecode(res.body);
    return data['is_match'] == true;
  }

  // --- MAPS ---
  static Future<List<Map<String, dynamic>>> getNearbyPlaces(
      double lat, double lon) async {
    final res = await http.post(
      Uri.parse('$baseUrl/maps/nearby'),
      headers: _headers,
      body: jsonEncode({'lat': lat, 'lon': lon, 'radius_m': 1500}),
    );
    if (res.statusCode == 200) {
      final data = jsonDecode(res.body);
      return List<Map<String, dynamic>>.from(data['places']);
    }
    return [];
  }
}