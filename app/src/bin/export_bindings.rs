fn main() {
    let builder = todo_app_lib::commands::get_specta_builder();
    builder
        .export(
            specta_typescript::Typescript::default(),
            "../client/src/lib/bindings.ts",
        )
        .expect("Failed to export specta typescript bindings");
}
