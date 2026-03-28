import 'package:flutter/material.dart';
import 'package:flutter_card_swiper/flutter_card_swiper.dart';
import 'package:google_fonts/google_fonts.dart';
import 'package:url_launcher/url_launcher.dart';
import '../models/student.dart';
import '../services/api_service.dart';
import 'widgets/student_card.dart';

class SwipeScreen extends StatefulWidget {
  const SwipeScreen({super.key});

  @override
  State<SwipeScreen> createState() => _SwipeScreenState();
}

class _SwipeScreenState extends State<SwipeScreen> {
  final CardSwiperController _swiperController = CardSwiperController();
  List<Student> _students = [];
  bool _loading = true;
  String? _error;
  String? _matchMessage;

  @override
  void initState() {
    super.initState();
    _loadStudents();
  }

  Future<void> _loadStudents() async {
    try {
      final students = await ApiService.getRecommendations();
      setState(() {
        _students = students;
        _loading = false;
      });
    } catch (e) {
      setState(() {
        _error = e.toString();
        _loading = false;
      });
    }
  }

  Future<void> _openMaps() async {
    // Coordinate di esempio (Napoli centro) — 
    // sostituibili con geolocalizzazione reale
    const double lat = 40.8518;
    const double lon = 14.2681;

    final places = await ApiService.getNearbyPlaces(lat, lon);

    if (!mounted) return;

    if (places.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Nessun posto trovato vicino a te')),
      );
      return;
    }

    // Apri il primo posto trovato su Google Maps
    final place = places.first;
    final uri = Uri.parse(
      'https://www.google.com/maps/search/?api=1&query=${place['lat']},${place['lon']}',
    );
    if (await canLaunchUrl(uri)) {
      await launchUrl(uri, mode: LaunchMode.externalApplication);
    }
  }

  bool _onSwipe(int prev, int? curr, CardSwiperDirection dir) {
    final student = _students[prev];
    final isLike = dir == CardSwiperDirection.right;

    ApiService.swipe(student.id, isLike).then((isMatch) {
      if (isMatch && mounted) {
        setState(() => _matchMessage = '🎉 Match con ${student.displayName}!');
        Future.delayed(const Duration(seconds: 3), () {
          if (mounted) setState(() => _matchMessage = null);
        });
      }
    });

    return true;
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFF0F0F1A),
      appBar: AppBar(
        backgroundColor: Colors.transparent,
        elevation: 0,
        title: Text(
          'Study Match',
          style: GoogleFonts.poppins(
            fontWeight: FontWeight.bold,
            color: Colors.white,
            fontSize: 22,
          ),
        ),
        centerTitle: true,
        actions: [
          IconButton(
            onPressed: _openMaps,
            icon: const Icon(Icons.map_rounded, color: Color(0xFF6C63FF)),
            tooltip: 'Posti studio vicini',
          ),
          const SizedBox(width: 8),
        ],
      ),
      body: Stack(
        children: [
          if (_loading)
            const Center(child: CircularProgressIndicator(
              color: Color(0xFF6C63FF),
            ))
          else if (_error != null)
            Center(
              child: Padding(
                padding: const EdgeInsets.all(24),
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    const Icon(Icons.error_outline,
                        color: Colors.redAccent, size: 48),
                    const SizedBox(height: 16),
                    Text(_error!,
                        textAlign: TextAlign.center,
                        style: const TextStyle(color: Colors.white70)),
                    const SizedBox(height: 16),
                    ElevatedButton(
                      onPressed: () {
                        setState(() { _loading = true; _error = null; });
                        _loadStudents();
                      },
                      child: const Text('Riprova'),
                    )
                  ],
                ),
              ),
            )
          else if (_students.isEmpty)
            Center(
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  const Icon(Icons.people_outline,
                      color: Colors.white38, size: 64),
                  const SizedBox(height: 16),
                  Text(
                    'Nessuno studente compatibile\nper ora!',
                    textAlign: TextAlign.center,
                    style: GoogleFonts.poppins(
                        color: Colors.white54, fontSize: 16),
                  ),
                ],
              ),
            )
          else
            Column(
              children: [
                Expanded(
                  child: CardSwiper(
                    controller: _swiperController,
                    cardsCount: _students.length,
                    onSwipe: _onSwipe,
                    padding: const EdgeInsets.symmetric(
                        horizontal: 20, vertical: 16),
                    cardBuilder: (ctx, index, h, v) =>
                        StudentCard(student: _students[index]),
                  ),
                ),

                // Bottoni like/dislike
                Padding(
                  padding: const EdgeInsets.only(bottom: 32, top: 8),
                  child: Row(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      _ActionButton(
                        icon: Icons.close,
                        color: Colors.redAccent,
                        onTap: () => _swiperController.swipe(
                            CardSwiperDirection.left),
                      ),
                      const SizedBox(width: 32),
                      _ActionButton(
                        icon: Icons.favorite,
                        color: const Color(0xFF6C63FF),
                        onTap: () => _swiperController.swipe(
                            CardSwiperDirection.right),
                        large: true,
                      ),
                      const SizedBox(width: 32),
                      _ActionButton(
                        icon: Icons.map_rounded,
                        color: const Color(0xFF43C6AC),
                        onTap: _openMaps,
                      ),
                    ],
                  ),
                ),
              ],
            ),

          // Match banner
          if (_matchMessage != null)
            Positioned(
              top: 20,
              left: 24,
              right: 24,
              child: Container(
                padding: const EdgeInsets.symmetric(
                    vertical: 14, horizontal: 20),
                decoration: BoxDecoration(
                  color: const Color(0xFF6C63FF),
                  borderRadius: BorderRadius.circular(16),
                  boxShadow: [
                    BoxShadow(
                      color: const Color(0xFF6C63FF).withOpacity(0.5),
                      blurRadius: 20,
                    )
                  ],
                ),
                child: Text(
                  _matchMessage!,
                  textAlign: TextAlign.center,
                  style: GoogleFonts.poppins(
                    color: Colors.white,
                    fontWeight: FontWeight.bold,
                    fontSize: 16,
                  ),
                ),
              ),
            ),
        ],
      ),
    );
  }
}

class _ActionButton extends StatelessWidget {
  final IconData icon;
  final Color color;
  final VoidCallback onTap;
  final bool large;

  const _ActionButton({
    required this.icon,
    required this.color,
    required this.onTap,
    this.large = false,
  });

  @override
  Widget build(BuildContext context) {
    final size = large ? 72.0 : 56.0;
    return GestureDetector(
      onTap: onTap,
      child: Container(
        width: size,
        height: size,
        decoration: BoxDecoration(
          shape: BoxShape.circle,
          color: color.withOpacity(0.12),
          border: Border.all(color: color.withOpacity(0.4), width: 2),
          boxShadow: [
            BoxShadow(
              color: color.withOpacity(0.2),
              blurRadius: