mod simd;

struct Row<const PS: usize> {

}

trait DataFrame<const PS: usize> {
    // fn from_cursor
    fn next(&self) -> Row<PS>;
}