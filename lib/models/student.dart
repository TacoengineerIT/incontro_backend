class Student {
  final String id;
  final String email;
  final List<String> studySubjects;
  final String learningStyle;
  final int score;

  Student({
    required this.id,
    required this.email,
    required this.studySubjects,
    required this.learningStyle,
    required this.score,
  });

  String get displayName => email.split('@').first;

  factory Student.fromJson(Map<String, dynamic> json) {
    final user = json['user'] as Map<String, dynamic>;
    return Student(
      id: user['id'] as String,
      email: user['email'] as String,
      studySubjects: List<String>.from(user['study_subjects'] ?? []),
      learningStyle: user['learning_style'] ?? 'Non specificato',
      score: json['score'] as int? ?? 0,
    );
  }
}